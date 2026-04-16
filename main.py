import os
import jwt
import asyncpg
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Depends, Query, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from passlib.context import CryptContext
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

# =========================
# 🚀 APP
# =========================
app = FastAPI(title="Secure Football API")

# =========================
# 🔐 SECURITY
# =========================
security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# =========================
# 🚫 RATE LIMIT
# =========================
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request, exc):
    raise HTTPException(status_code=429, detail="Too many requests")

# =========================
# 🌍 ENV
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_USER = os.getenv("ADMIN_USER")
ADMIN_PASS_HASH = os.getenv("ADMIN_PASS_HASH")
SECRET_KEY = os.getenv("SECRET_KEY")

if not DATABASE_URL or not ADMIN_USER or not ADMIN_PASS_HASH or not SECRET_KEY:
    raise Exception("ENV eksik!")

# =========================
# 🗄 DATABASE
# =========================
pool = None

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

@app.on_event("shutdown")
async def shutdown():
    await pool.close()

# =========================
# 🧠 AUDIT LOG
# =========================
audit_logs = []

def log_event(event, user=None, ip=None):
    audit_logs.append({
        "event": event,
        "user": user,
        "ip": ip,
        "time": datetime.utcnow().isoformat()
    })

# =========================
# 🔑 AUTH
# =========================
def verify_password(plain, hashed):
    return pwd_context.verify(plain, hashed)

def create_token(username, role):
    return jwt.encode(
        {
            "user": username,
            "role": role,
            "exp": datetime.utcnow() + timedelta(hours=12)
        },
        SECRET_KEY,
        algorithm="HS256"
    )

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        return jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except:
        raise HTTPException(401, "Invalid token")

def require_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(403, "Admin only")
    return user

# =========================
# 🔑 LOGIN
# =========================
class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/login")
@limiter.limit("5/minute")
def login(request: Request, data: LoginRequest):

    ip = get_remote_address(request)

    if data.username != ADMIN_USER:
        log_event("failed_login", data.username, ip)
        raise HTTPException(401, "Wrong username")

    if not verify_password(data.password, ADMIN_PASS_HASH):
        log_event("failed_login", data.username, ip)
        raise HTTPException(401, "Wrong password")

    token = create_token(data.username, "admin")
    log_event("successful_login", data.username, ip)

    return {"access_token": token}

# =========================
# 🌍 HELPERS
# =========================
def format_country(country):
    return country.replace("-", " ").title()

def format_league(country, league):
    return f"{format_country(country)}: {league.replace('-', ' ').title()}"

def format_percent(value):
    return f"{round(value * 100, 1)}%"

# =========================
# 🧪 HEALTH
# =========================
@app.get("/")
async def health():
    return {"status": "ok"}

# =========================
# 📊 STATS
# =========================
@app.get("/stats")
async def stats(user=Depends(get_current_user)):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE has_odds = true) as with_odds
            FROM matches
        """)
    return {
        "total_matches": row["total"],
        "with_odds": row["with_odds"]
    }

# =========================
# ⚽ MATCHES
# =========================
@app.get("/matches")
async def get_matches(user=Depends(get_current_user), limit: int = 50):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT country, league, home_team, away_team,
                   ft_home, ft_away, has_odds
            FROM matches
            ORDER BY match_id DESC
            LIMIT $1
        """, limit)

    result = []
    for r in rows:
        base = {
            "match": f"{r['home_team']} vs {r['away_team']}",
            "score": f"{r['ft_home']}-{r['ft_away']}" if r["ft_home"] else None
        }

        if user["role"] == "admin":
            base["country"] = r["country"]
            base["league"] = r["league"]
            base["has_odds"] = r["has_odds"]

        result.append(base)

    return result

# =========================
# 🏆 LEAGUES
# =========================
@app.get("/leagues/summary")
async def leagues_summary(user=Depends(get_current_user), country: str = None):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT league, COUNT(*) as total
            FROM matches
            WHERE country = $1
            GROUP BY league
        """, country.lower())

    return {
        "country": format_country(country),
        "leagues": [
            {"league": r["league"], "total": r["total"]}
            for r in rows
        ]
    }

# =========================
# 🔒 ADMIN
# =========================
@app.get("/admin/tables")
async def get_tables(user=Depends(require_admin)):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
    return {"tables": [r["table_name"] for r in rows]}

@app.get("/admin/logs")
def get_logs(user=Depends(require_admin)):
    return {"logs": audit_logs[-50:]}

@app.get("/admin/indexes")
async def get_indexes(user=Depends(require_admin)):
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

@app.get("/admin/columns")
async def get_columns(table: str, user=Depends(require_admin)):
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
    user=Depends(get_current_user),
    country: str = None,
    team: str = None
):
    if not country or not team:
        raise HTTPException(400, "country and team required")

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT home_team, away_team,
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

    score_dist = {"0_1": 0, "2_3": 0, "4_5": 0, "6_plus": 0}
    score_patterns = {"home": {}, "away": {}}

    def result(s, c):
        return "W" if s > c else "L" if s < c else "D"

    for r in rows:
        is_home = r["home_team"] == team

        s = r["ft_home"] if is_home else r["ft_away"]
        c = r["ft_away"] if is_home else r["ft_home"]

        scored += s
        conceded += c

        res = result(s, c)

        if res == "W": wins += 1
        elif res == "L": losses += 1
        else: draws += 1

        # GOAL DIST
        if s <= 1: score_dist["0_1"] += 1
        elif s <= 3: score_dist["2_3"] += 1
        elif s <= 5: score_dist["4_5"] += 1
        else: score_dist["6_plus"] += 1

        # SCORE PATTERN
        pattern = f"{r['ft_home']}-{r['ft_away']}"
        role = "home" if is_home else "away"

        score_patterns[role][pattern] = score_patterns[role].get(pattern, 0) + 1

    def avg(a, b): return round(a / b, 2) if b else 0

    response = {
        "team": team,
        "played": played,
        "wins": wins,
        "draws": draws,
        "losses": losses,
        "avg_scored": avg(scored, played),
        "avg_conceded": avg(conceded, played),
        "scoring_distribution": score_dist,
        "patterns": score_patterns
    }

    # 🔐 ADMIN EXTRA
    if user["role"] == "admin":
        response["total_goals"] = scored
        response["total_conceded"] = conceded

    return response

@app.get("/league/stats", tags=["Analysis"])
async def league_stats(
    user=Depends(get_current_user),
    country: str = None,
    league: str = None
):
    if not country or not league:
        raise HTTPException(400, "country and league required")

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ft_home, ft_away
            FROM matches
            WHERE country = $1 AND league = $2
        """, country.lower(), league.lower())

    if not rows:
        return {"error": "No data"}

    total = len(rows)
    home_wins = draws = away_wins = 0
    total_goals = 0

    goal_dist = {"0_1": 0, "2_3": 0, "4_5": 0, "6_plus": 0}

    for r in rows:
        h, a = r["ft_home"], r["ft_away"]

        total_goals += h + a

        if h > a: home_wins += 1
        elif h < a: away_wins += 1
        else: draws += 1

        g = h + a
        if g <= 1: goal_dist["0_1"] += 1
        elif g <= 3: goal_dist["2_3"] += 1
        elif g <= 5: goal_dist["4_5"] += 1
        else: goal_dist["6_plus"] += 1

    def rate(x): return round((x / total) * 100, 1) if total else 0

    response = {
        "league": format_league(country, league),
        "matches": total,
        "home_win_rate": rate(home_wins),
        "draw_rate": rate(draws),
        "away_win_rate": rate(away_wins),
        "avg_goals": round(total_goals / total, 2),
        "goal_distribution": goal_dist
    }

    # 🔐 ADMIN EXTRA
    if user["role"] == "admin":
        response["raw_counts"] = {
            "home_wins": home_wins,
            "draws": draws,
            "away_wins": away_wins
        }

    return response
