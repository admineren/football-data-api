import os
import asyncpg
from fastapi import FastAPI, Query

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL bulunamadı!")

pool = None


# 🚀 STARTUP → pool oluştur
@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        statement_cache_size=0  # 🔥 pgbouncer fix
    )


# 🔚 SHUTDOWN → kapat
@app.on_event("shutdown")
async def shutdown():
    await pool.close()

# Ülke Format
def format_country(country):
    return country.replace("-", " ").title()


# 🎯 league format
def format_league(country, league):
    country = country.replace("-", " ").title()
    league = league.replace("-", " ").title()
    return f"{country}: {league}"


# 🏠 root
@app.get("/")
async def home():
    return {"status": "running"}


# 🧪 sağlık
@app.get("/health")
async def health():
    try:
        async with pool.acquire() as conn:
            await conn.fetch("SELECT 1")
        return {"database": "connected"}
    except Exception as e:
        return {"database": "error", "detail": str(e)}


# 📊 toplam
@app.get("/stats")
async def stats():
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(*) FROM matches")
        return {"total_matches": row[0]}
    except Exception as e:
        return {"error": str(e)}


# MATCHES DB - PAGINATION 
@app.get("/matches")
async def get_matches(
    country: str = None,
    limit: int = Query(50, le=200),
    offset: int = 0
):
    try:
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
                "league": format_league(r["country"], r["league"]),
                "match": f"{r['home_team']} vs {r['away_team']}",
                "ht_score": f"{r['ht_home']}-{r['ht_away']}" if r["ht_home"] is not None else None,
                "ft_score": f"{r['ft_home']}-{r['ft_away']}" if r["ft_home"] is not None else None,
                "has_odds": r["has_odds"]
            }
            for r in rows
        ]

    except Exception as e:
        return {"error": str(e)}

# LEAGUE SUMMARY - COUNTRY AND LEAGUES
@app.get("/leagues/summary")
async def leagues_summary(country: str):
    try:
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
                    "no_odds": row["no_odds"]
                }
                for row in rows
            ]
        }

    except Exception as e:
        return {"error": str(e)}
        
