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


# 🏠 root
@app.get("/")
def home():
    return {"status": "running"}


# 🧪 DB sağlık kontrolü
@app.get("/health")
def health():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return {"database": "connected"}
    except Exception as e:
        return {"database": "error", "detail": str(e)}


# 📊 toplam maç sayısı
@app.get("/stats")
def stats():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM matches;")
        total = cur.fetchone()[0]

        cur.close()
        conn.close()

        return {
            "total_matches": total
        }

    except Exception as e:
        return {"error": str(e)}


# ⚽ maç listesi (sade)
@app.get("/matches")
def get_matches():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
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

        rows = cur.fetchall()

        cur.close()
        conn.close()

        data = []

        for r in rows:
            data.append({
                "league": format_league(r[0], r[1]),
                "match": f"{r[2]} vs {r[3]}",
                "ht_score": f"{r[4]}-{r[5]}" if r[4] is not None else None,
                "ft_score": f"{r[6]}-{r[7]}" if r[6] is not None else None,
                "has_odds": r[8]
            })

        return data

    except Exception as e:
        return {"error": str(e)}

@app.get("/leagues/summary")
async def leagues_summary(country: str):
    conn = await asyncpg.connect(DATABASE_URL)

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
