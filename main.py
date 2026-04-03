import os
import asyncpg
from fastapi import FastAPI

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL bulunamadı!")


# 🎯 league format
def format_league(country, league):
    return f"{country.title()}: {league.replace('-', ' ').title()}"


# 🔌 DB bağlantı
async def get_conn():
    return await asyncpg.connect(DATABASE_URL)


# 🏠 root
@app.get("/")
async def home():
    return {"status": "running"}


# 🧪 DB sağlık kontrolü
@app.get("/health")
async def health():
    try:
        conn = await get_conn()

        await conn.fetch("SELECT 1")

        await conn.close()

        return {"database": "connected"}

    except Exception as e:
        return {"database": "error", "detail": str(e)}


# 📊 toplam maç sayısı
@app.get("/stats")
async def stats():
    try:
        conn = await get_conn()

        row = await conn.fetchrow("SELECT COUNT(*) FROM matches")

        await conn.close()

        return {
            "total_matches": row[0]
        }

    except Exception as e:
        return {"error": str(e)}


# ⚽ maç listesi (sade)
@app.get("/matches")
async def get_matches():
    try:
        conn = await get_conn()

        rows = await conn.fetch("""
            SELECT 
                country,
                league,
                home_team,
                away_team,
                ht_home,
                ht_away,
                ft_home,
                ft_away,
                has_odds
            FROM matches
            ORDER BY match_id DESC
            LIMIT 50;
        """)

        await conn.close()

        data = []

        for r in rows:
            data.append({
                "league": format_league(r["country"], r["league"]),
                "match": f"{r['home_team']} vs {r['away_team']}",
                "ht_score": f"{r['ht_home']}-{r['ht_away']}" if r["ht_home"] is not None else None,
                "ft_score": f"{r['ft_home']}-{r['ft_away']}" if r["ft_home"] is not None else None,
                "has_odds": r["has_odds"]
            })

        return data

    except Exception as e:
        return {"error": str(e)}


# 📊 lig özet
@app.get("/leagues/summary")
async def leagues_summary(country: str):
    try:
        conn = await get_conn()

        rows = await conn.fetch("""
            SELECT 
                league,
                COUNT(*) as total_matches,
                COUNT(*) FILTER (WHERE has_odds = true) as with_odds,
                COUNT(*) FILTER (WHERE has_odds = false) as no_odds
            FROM matches
            WHERE country = $1
            GROUP BY league
            ORDER BY total_matches DESC;
        """, country.lower())

        await conn.close()

        return {
            "country": country.title(),
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
