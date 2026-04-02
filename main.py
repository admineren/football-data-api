from fastapi import FastAPI
from db import get_conn
import os

app = FastAPI()


@app.get("/")
def home():
    return {"status": "running"}

@app.get("/debug")
def debug():
    import os
    return {"url": os.environ.get("DATABASE_URL")}


@app.get("/matches")
def get_matches():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT match_id, home_team, away_team, ft_home, ft_away
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
            "match_id": r[0],
            "home": r[1],
            "away": r[2],
            "score": f"{r[3]}-{r[4]}"
        })

    return data
