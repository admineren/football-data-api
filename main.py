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
                ht_home, ht_away,
                ht_ft
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

    # 🔥 NEW RANGE DIST
    score_dist = {
        "0_1": 0,
        "2_3": 0,
        "4_5": 0,
        "6_plus": 0
    }

    # ❌ overall kaldırıldı
    score_patterns = {"home": {}, "away": {}}

    ht_patterns = {}
    ht_ft_patterns = {"home": {}, "away": {}}

    # ================= HELPERS =================

    def team_result(s, c):
        if s > c: return "W"
        if s < c: return "L"
        return "D"

    def map_htft(x):
        return "1" if x == "H" else "0" if x == "D" else "2"

    # ================= LOOP =================

    for r in rows:

        is_home = r["home_team"] == team

        ft_h = r["ft_home"]
        ft_a = r["ft_away"]

        # TEAM PERSPECTIVE
        if is_home:
            s, c = ft_h, ft_a
            home_played += 1
            home_scored += s
            home_conceded += c
        else:
            s, c = ft_a, ft_h
            away_played += 1
            away_scored += s
            away_conceded += c

        res = team_result(s, c)

        # OVERALL
        scored += s
        conceded += c

        if res == "W": wins += 1
        elif res == "L": losses += 1
        else: draws += 1

        # HOME / AWAY RESULT
        if is_home:
            if res == "W": home_wins += 1
            elif res == "L": home_losses += 1
            else: home_draws += 1
        else:
            if res == "W": away_wins += 1
            elif res == "L": away_losses += 1
            else: away_draws += 1

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

        # ================= RANGE GOALS =================

        if s <= 1:
            score_dist["0_1"] += 1
        elif s <= 3:
            score_dist["2_3"] += 1
        elif s <= 5:
            score_dist["4_5"] += 1
        else:
            score_dist["6_plus"] += 1

        # ================= SCORE PATTERNS =================

        pattern = f"{ft_h}-{ft_a}"
        role = "home" if is_home else "away"

        if pattern not in score_patterns[role]:
            score_patterns[role][pattern] = {"count": 0, "result": res}

        score_patterns[role][pattern]["count"] += 1

        # ================= HT =================

        ht_h = r["ht_home"]
        ht_a = r["ht_away"]

        if ht_h is not None and ht_a is not None:

            if is_home:
                ht_s, ht_c = ht_h, ht_a
            else:
                ht_s, ht_c = ht_a, ht_h

            ht_res = team_result(ht_s, ht_c)
            ht_pattern = f"{ht_h}-{ht_a}"

            if ht_pattern not in ht_patterns:
                ht_patterns[ht_pattern] = {
                    "count": 0,
                    "result": ht_res
                }

            ht_patterns[ht_pattern]["count"] += 1

        # ================= HT/FT =================

        htft = r.get("ht_ft")

        if htft:
            ht, ft = htft.split("/")

            key = f"{map_htft(ht)}/{map_htft(ft)}"
            role = "home" if is_home else "away"

            ht_ft_patterns[role][key] = ht_ft_patterns[role].get(key, 0) + 1

    # ================= FORMAT =================

    def avg(a, b):
        return round(a / b, 2) if b else 0

    def rate(a, b):
        return round((a / b) * 100, 1) if b else 0

    def top5_patterns(d):
        return [
            {
                "score": k,
                "count": v["count"],
                "result": v["result"]
            }
            for k, v in sorted(d.items(), key=lambda x: x[1]["count"], reverse=True)[:5]
        ]

    def top5_ht(d):
        return [
            {
                "score": k,
                "count": v["count"],
                "result": v["result"]
            }
            for k, v in sorted(d.items(), key=lambda x: x[1]["count"], reverse=True)[:5]
        ]

    def top5_simple(d):
        return [
            {"pattern": k, "count": v}
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
            "home": top5_patterns(score_patterns["home"]),
            "away": top5_patterns(score_patterns["away"])
        },

        "ht_patterns": top5_ht(ht_patterns),

        "ht_ft_patterns": {
            "home": top5_simple(ht_ft_patterns["home"]),
            "away": top5_simple(ht_ft_patterns["away"])
        }
    }

@app.get("/league/stats", tags=["Analysis"])
async def league_stats(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    country: str = None,
    league: str = None
):
    check_token(credentials)

    if not country or not league:
        raise HTTPException(status_code=400, detail="country and league required")

    async with pool.acquire() as conn:

        # =========================
        # CORE + GOAL DISTRIBUTIONS
        # =========================
        core = await conn.fetchrow("""
            SELECT
                COUNT(*) as total,

                COUNT(*) FILTER (WHERE ft_home > ft_away) as home_wins,
                COUNT(*) FILTER (WHERE ft_home = ft_away) as draws,
                COUNT(*) FILTER (WHERE ft_home < ft_away) as away_wins,

                SUM(ft_home + ft_away) as total_goals,

                COUNT(*) FILTER (WHERE ft_home > 0 AND ft_away > 0) as btts,
                COUNT(*) FILTER (WHERE ft_home = 0 OR ft_away = 0) as clean_sheets,

                -- FT TOTAL GOALS
                COUNT(*) FILTER (WHERE (ft_home + ft_away) BETWEEN 0 AND 1) as ft_0_1,
                COUNT(*) FILTER (WHERE (ft_home + ft_away) BETWEEN 2 AND 3) as ft_2_3,
                COUNT(*) FILTER (WHERE (ft_home + ft_away) BETWEEN 4 AND 5) as ft_4_5,
                COUNT(*) FILTER (WHERE (ft_home + ft_away) >= 6) as ft_6_plus,

                -- HT TOTAL GOALS
                COUNT(*) FILTER (
                    WHERE ht_home IS NOT NULL
                      AND ht_away IS NOT NULL
                      AND (ht_home + ht_away) BETWEEN 0 AND 1
                ) as ht_0_1,

                COUNT(*) FILTER (
                    WHERE ht_home IS NOT NULL
                      AND ht_away IS NOT NULL
                      AND (ht_home + ht_away) BETWEEN 2 AND 3
                ) as ht_2_3,

                COUNT(*) FILTER (
                    WHERE ht_home IS NOT NULL
                      AND ht_away IS NOT NULL
                      AND (ht_home + ht_away) BETWEEN 4 AND 5
                ) as ht_4_5,

                COUNT(*) FILTER (
                    WHERE ht_home IS NOT NULL
                      AND ht_away IS NOT NULL
                      AND (ht_home + ht_away) >= 6
                ) as ht_6_plus,

                -- HOME TEAM GOALS
                COUNT(*) FILTER (WHERE ft_home BETWEEN 0 AND 1) as home_0_1,
                COUNT(*) FILTER (WHERE ft_home BETWEEN 2 AND 3) as home_2_3,
                COUNT(*) FILTER (WHERE ft_home BETWEEN 4 AND 5) as home_4_5,
                COUNT(*) FILTER (WHERE ft_home >= 6) as home_6_plus,

                -- AWAY TEAM GOALS
                COUNT(*) FILTER (WHERE ft_away BETWEEN 0 AND 1) as away_0_1,
                COUNT(*) FILTER (WHERE ft_away BETWEEN 2 AND 3) as away_2_3,
                COUNT(*) FILTER (WHERE ft_away BETWEEN 4 AND 5) as away_4_5,
                COUNT(*) FILTER (WHERE ft_away >= 6) as away_6_plus

            FROM matches
            WHERE country = $1
              AND league = $2
              AND ft_home IS NOT NULL
              AND ft_away IS NOT NULL
        """, country.lower(), league.lower())

        # =========================
        # ODDS
        # =========================
        odds = await conn.fetchrow("""
            SELECT
                COUNT(*) as total,

                COUNT(*) FILTER (
                    WHERE (
                        (home_odds < away_odds AND ft_home > ft_away) OR
                        (away_odds < home_odds AND ft_away > ft_home)
                    )
                ) as favorite_wins,

                COUNT(*) FILTER (
                    WHERE (
                        (home_odds < away_odds AND ft_home < ft_away) OR
                        (away_odds < home_odds AND ft_away < ft_home)
                    )
                ) as underdog_wins

            FROM matches
            WHERE country = $1
              AND league = $2
              AND has_odds = true
              AND home_odds IS NOT NULL
              AND away_odds IS NOT NULL
              AND ft_home IS NOT NULL
              AND ft_away IS NOT NULL
        """, country.lower(), league.lower())

        # =========================
        # HT/FT VALID MATCHES ONLY
        # =========================
        ht_rows = await conn.fetch("""
            SELECT
                ht_home,
                ht_away,
                ft_home,
                ft_away
            FROM matches
            WHERE country = $1
              AND league = $2
              AND ht_home IS NOT NULL
              AND ht_away IS NOT NULL
              AND ft_home IS NOT NULL
              AND ft_away IS NOT NULL
        """, country.lower(), league.lower())

        # =========================
        # HELPERS
        # =========================
        def rate(a, b):
            return (a / b) if b else 0

        def percent(v):
            return f"{round(v * 100, 1)}%"

        def result_code(home, away):
            if home > away:
                return "1"
            elif home < away:
                return "2"
            return "0"

        # =========================
        # BASE VALUES
        # =========================
        total = core["total"] or 0
        total_goals = core["total_goals"] or 0

        home_wins = core["home_wins"] or 0
        draws = core["draws"] or 0
        away_wins = core["away_wins"] or 0

        home_rate = rate(home_wins, total)
        draw_rate = rate(draws, total)
        away_rate = rate(away_wins, total)

        avg_goals = round(total_goals / total, 2) if total else 0
        home_advantage = round((home_rate - away_rate) * 100, 1)

        clean_rate = rate(core["clean_sheets"], total)
        btts_rate = rate(core["btts"], total)

        # =========================
        # FT / HT GOAL DISTRIBUTION
        # =========================
        ft_goal_distribution = {
            "0_1": core["ft_0_1"] or 0,
            "2_3": core["ft_2_3"] or 0,
            "4_5": core["ft_4_5"] or 0,
            "6_plus": core["ft_6_plus"] or 0
        }

        ht_goal_distribution = {
            "0_1": core["ht_0_1"] or 0,
            "2_3": core["ht_2_3"] or 0,
            "4_5": core["ht_4_5"] or 0,
            "6_plus": core["ht_6_plus"] or 0
        }

        home_goal_distribution = {
            "0_1": core["home_0_1"] or 0,
            "2_3": core["home_2_3"] or 0,
            "4_5": core["home_4_5"] or 0,
            "6_plus": core["home_6_plus"] or 0
        }

        away_goal_distribution = {
            "0_1": core["away_0_1"] or 0,
            "2_3": core["away_2_3"] or 0,
            "4_5": core["away_4_5"] or 0,
            "6_plus": core["away_6_plus"] or 0
        }

        # =========================
        # HT/FT DISTRIBUTION
        # =========================
        htft_distribution = {}

        for row in ht_rows:
            ht_result = result_code(row["ht_home"], row["ht_away"])
            ft_result = result_code(row["ft_home"], row["ft_away"])

            key = f"{ht_result}/{ft_result}"
            htft_distribution[key] = htft_distribution.get(key, 0) + 1

        # büyükten küçüğe sırala
        htft_distribution = dict(
            sorted(
                htft_distribution.items(),
                key=lambda x: x[1],
                reverse=True
            )
        )

        # =========================
        # ODDS CALC
        # =========================
        odds_total = odds["total"] or 0

        favorite_win_rate = percent(
            rate(odds["favorite_wins"] or 0, odds_total)
        )

        underdog_win_rate = percent(
            rate(odds["underdog_wins"] or 0, odds_total)
        )

        # =========================
        # RESPONSE
        # =========================
        return {
            "league": format_league(country, league),

            "structure": {
                "matches": total,
                "results": {
                    "home_wins": home_wins,
                    "draws": draws,
                    "away_wins": away_wins,
                    "home_win_rate": percent(home_rate),
                    "draw_rate": percent(draw_rate),
                    "away_win_rate": percent(away_rate)
                },
                "home_advantage": {
                    "advantage": f"{home_advantage}%"
                }
            },

            "scoring": {
                "avg_goals": avg_goals,
                "clean_sheet_rate": percent(clean_rate),
                "btts_rate": percent(btts_rate),

                "ft_goal_distribution": ft_goal_distribution,
                "ht_goal_distribution": ht_goal_distribution,

                "team_goal_distribution": {
                    "home": home_goal_distribution,
                    "away": away_goal_distribution
                }
            },

            "predictability": {
                "favorite_win_rate": favorite_win_rate,
                "underdog_win_rate": underdog_win_rate
            },

            "game_flow": {
                "ht_ft_distribution": htft_distribution
            }
        }
