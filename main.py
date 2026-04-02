from fastapi import FastAPI
import psycopg2
import os

app = FastAPI()

def get_conn():
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        raise Exception("DATABASE_URL bulunamadı")

    return psycopg2.connect(
        db_url,
        sslmode="require",
        connect_timeout=10
    )

@app.get("/")
def home():
    return {"status": "running"}

@app.get("/test-db")
def test_db():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/matches")
def get_matches():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT match_id, home_team, away_team, ft_home, ft_away
            FROM matches
            ORDER BY match_id DESC
            LIMIT 20;
        """)

        rows = cur.fetchall()

        cur.close()
        conn.close()

        return [
            {
                "match_id": r[0],
                "home": r[1],
                "away": r[2],
                "score": f"{r[3]}-{r[4]}"
            }
            for r in rows
        ]

    except Exception as e:
        return {"error": str(e)}
