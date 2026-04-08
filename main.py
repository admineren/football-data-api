import os
import asyncpg
import jwt
from datetime import datetime, timedelta
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

app = FastAPI(
    title="Football Data API",
    description="Secure football data API with admin tools",
    version="1.0.0"
)

# 🔐 SECURITY
security = HTTPBearer()

# ENV
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_USER = os.getenv("ADMIN_USER")
ADMIN_PASS = os.getenv("ADMIN_PASS")
SECRET_KEY = os.getenv("SECRET_KEY")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL bulunamadı!")

pool = None


# 🚀 STARTUP
@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        statement_cache_size=0
    )


# 🔚 SHUTDOWN
@app.on_event("shutdown")
async def shutdown():
    await pool.close()


# 🔐 TOKEN KONTROL
def check_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except:
        raise HTTPException(status_code=401, detail="Invalid token")


# 🔑 LOGIN MODEL
class LoginRequest(BaseModel):
    username: str
    password: str


# 🔑 LOGIN
@app.post("/login", tags=["Auth"])
def login(data: LoginRequest):

    if data.username != ADMIN_USER or data.password != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Wrong credentials")

    token = jwt.encode(
        {
            "user": data.username,
            "exp": datetime.utcnow() + timedelta(hours=12)
        },
        SECRET_KEY,
        algorithm="HS256"
    )

    return {"access_token": token}


# 🌍 HELPERS
def format_country(country):
    return country.replace("-", " ").title()


def format_league(country, league):
    return f"{format_country(country)}: {league.replace('-', ' ').title()}"


def format_percent(value):
    return f"{round(value * 100, 1)}%"


# 🧪 HEALTH
@app.get("/", tags=["System"])
async def health():
    try:
        async with pool.acquire() as conn:
            await conn.fetch("SELECT 1")
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# 📊 STATS
@app.get("/stats", tags=["Stats"])
async def stats(credentials: HTTPAuthorizationCredentials = Depends(security)):
    check_token(credentials)

    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE has_odds = true) as with_odds,
                COUNT(*) FILTER (WHERE has_odds = false) as no_odds
            FROM matches
        """)

    total = row["total"]
    with_odds = row["with_odds"]
    coverage = (with_odds / total) if total > 0 else 0

    return {
        "total_matches": total,
        "with_odds": with_odds,
        "no_odds": row["no_odds"],
        "odds_coverage": format_percent(coverage)
    }


# ⚽ MATCHES
@app.get("/matches", tags=["Matches"])
async def get_matches(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    country: str = None,
    limit: int = Query(50, le=200),
    offset: int = 0
):
    check_token(credentials)

    async with pool.acquire() as conn:

        if country:
            rows = await conn.fetch("""
                SELECT country, league, home_team, away_team,
                       ht_home, ht_away, ft_home, ft_away, has_odds
                FROM matches
                WHERE country = $1
                ORDER BY match_id DESC
                LIMIT $2 OFFSET $3
            """, country.lower(), limit, offset)
        else:
            rows = await conn.fetch("""
                SELECT country, league, home_team, away_team,
                       ht_home, ht_away, ft_home, ft_away, has_odds
                FROM matches
                ORDER BY match_id DESC
                LIMIT $1 OFFSET $2
            """, limit, offset)

    return [
        {
            "country": format_country(r["country"]),
            "league": format_league(r["country"], r["league"]),
            "match": f"{r['home_team']} vs {r['away_team']}",
            "ht_score": f"{r['ht_home']}-{r['ht_away']}" if r["ht_home"] is not None else None,
            "ft_score": f"{r['ft_home']}-{r['ft_away']}" if r["ft_home"] is not None else None,
            "has_odds": r["has_odds"]
        }
        for r in rows
    ]


# 🏆 LEAGUE SUMMARY
@app.get("/leagues/summary", tags=["Leagues"])
async def leagues_summary(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    country: str = None
):
    check_token(credentials)

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                league,
                COUNT(*) as total_matches,
                COUNT(*) FILTER (WHERE has_odds = true) as with_odds,
                COUNT(*) FILTER (WHERE has_odds = false) as no_odds
            FROM matches
            WHERE country = $1
            GROUP BY league
            ORDER BY total_matches DESC
        """, country.lower())

    return {
        "country": format_country(country),
        "leagues": [
            {
                "league": row["league"].replace("-", " ").title(),
                "total_matches": row["total_matches"],
                "with_odds": row["with_odds"],
                "no_odds": row["no_odds"],
                "coverage": format_percent(
                    (row["with_odds"] / row["total_matches"])
                    if row["total_matches"] > 0 else 0
                )
            }
            for row in rows
        ]
    }


# =========================
# 🛠 ADMIN ENDPOINTS
# =========================

# 📋 TABLES
@app.get("/admin/tables", tags=["Admin"])
async def get_tables(credentials: HTTPAuthorizationCredentials = Depends(security)):
    check_token(credentials)

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)

    return {"tables": [r["table_name"] for r in rows]}


# 📊 INDEXES
@app.get("/admin/indexes", tags=["Admin"])
async def get_indexes(credentials: HTTPAuthorizationCredentials = Depends(security)):
    check_token(credentials)

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
        """)

    return [
        {
            "name": r["indexname"],
            "definition": r["indexdef"]
        }
        for r in rows
    ]


# 🧱 COLUMNS
@app.get("/admin/columns", tags=["Admin"])
async def get_columns(
    table: str,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    check_token(credentials)

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = $1
        """, table)

    return [
        {
            "column": r["column_name"],
            "type": r["data_type"]
        }
        for r in rows
    ]


@app.get("/team/stats", tags=["Analysis"])
async def team_stats(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    country: str = None,
    team: str = None
):
    check_token(credentials)

    if not country or not team:
        raise HTTPException(status_code=400, detail="country and team required")

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                home_team, away_team,
                ft_home, ft_away,
                ht_home, ht_away
            FROM matches
            WHERE country = $1
            AND (home_team = $2 OR away_team = $2)
        """, country.lower(), team)

    if not rows:
        return {"error": "No data"}

    # ================= INIT =================
    played = len(rows)
    wins = draws = losses = 0
    scored = conceded = 0

    home_played = home_wins = home_draws = home_losses = 0
    home_scored = home_conceded = 0

    away_played = away_wins = away_draws = away_losses = 0
    away_scored = away_conceded = 0

    clean_total = clean_home = clean_away = 0
    btts_total = btts_home = btts_away = 0

    score_dist = {"0": 0, "1": 0, "2": 0, "3_plus": 0}

    score_patterns = {"overall": {}, "home": {}, "away": {}}
    ht_patterns = {}

    ht_count = 0  # sadece HT olan maçlar

    # ================= LOOP =================
    for r in rows:

        is_home = r["home_team"] == team

        ft_h = r["ft_home"]
        ft_a = r["ft_away"]

        if is_home:
            s, c = ft_h, ft_a
            home_played += 1
            home_scored += s
            home_conceded += c

            if s > c: home_wins += 1
            elif s < c: home_losses += 1
            else: home_draws += 1

        else:
            s, c = ft_a, ft_h
            away_played += 1
            away_scored += s
            away_conceded += c

            if s > c: away_wins += 1
            elif s < c: away_losses += 1
            else: away_draws += 1

        # overall
        scored += s
        conceded += c

        if s > c: wins += 1
        elif s < c: losses += 1
        else: draws += 1

        # CLEAN SHEET
        if c == 0:
            clean_total += 1
            if is_home: clean_home += 1
            else: clean_away += 1

        # BTTS
        if s > 0 and c > 0:
            btts_total += 1
            if is_home: btts_home += 1
            else: btts_away += 1

        # SCORE DIST
        if s == 0: score_dist["0"] += 1
        elif s == 1: score_dist["1"] += 1
        elif s == 2: score_dist["2"] += 1
        else: score_dist["3_plus"] += 1

        # ================= FT PATTERN (ORIGINAL SCORE) =================
        pattern = f"{ft_h}-{ft_a}"

        def add_pattern(bucket, key):
            bucket[key] = bucket.get(key, 0) + 1

        add_pattern(score_patterns["overall"], pattern)
        add_pattern(score_patterns["home" if is_home else "away"], pattern)

        # ================= HT PATTERN =================
        ht_h = r["ht_home"]
        ht_a = r["ht_away"]

        if ht_h is not None and ht_a is not None:
            ht_count += 1
            ht_pattern = f"{ht_h}-{ht_a}"
            ht_patterns[ht_pattern] = ht_patterns.get(ht_pattern, 0) + 1

    # ================= HELPERS =================
    def avg(a, b):
        return round(a / b, 2) if b else 0

    def rate(a, b):
        return round((a / b) * 100, 1) if b else 0

    def get_result(score):
        h, a = map(int, score.split("-"))
        if h > a: return "H"
        elif h < a: return "A"
        return "D"

    def top5_array(d):
        return [
            {
                "score": k,
                "count": v,
                "result": get_result(k)
            }
            for k, v in sorted(d.items(), key=lambda x: x[1], reverse=True)[:5]
        ]

    def top5_simple(d):
        return [
            {"score": k, "count": v}
            for k, v in sorted(d.items(), key=lambda x: x[1], reverse=True)[:5]
        ]

    # ================= RESPONSE =================
    return {
        "team": team,
        "country": country,

        "overall": {
            "played": played,
            "wins": wins,
            "draws": draws,
            "losses": losses,

            "total_goals_scored": scored,
            "total_goals_conceded": conceded,

            "avg_scored": avg(scored, played),
            "avg_conceded": avg(conceded, played),

            "clean_sheet_rate": rate(clean_total, played),
            "btts_rate": rate(btts_total, played)
        },

        "home": {
            "played": home_played,
            "wins": home_wins,
            "draws": home_draws,
            "losses": home_losses,

            "total_goals_scored": home_scored,
            "total_goals_conceded": home_conceded,

            "avg_scored": avg(home_scored, home_played),
            "avg_conceded": avg(home_conceded, home_played),

            "clean_sheet_rate": rate(clean_home, home_played),
            "btts_rate": rate(btts_home, home_played)
        },

        "away": {
            "played": away_played,
            "wins": away_wins,
            "draws": away_draws,
            "losses": away_losses,

            "total_goals_scored": away_scored,
            "total_goals_conceded": away_conceded,

            "avg_scored": avg(away_scored, away_played),
            "avg_conceded": avg(away_conceded, away_played),

            "clean_sheet_rate": rate(clean_away, away_played),
            "btts_rate": rate(btts_away, away_played)
        },

        "scoring_distribution": score_dist,

        "score_patterns": {
            "overall": top5_array(score_patterns["overall"]),
            "home": top5_array(score_patterns["home"]),
            "away": top5_array(score_patterns["away"])
        },

        "ht_patterns": top5_simple(ht_patterns) if ht_count > 0 else []
    }
