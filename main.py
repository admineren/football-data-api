from fastapi import FastAPI
import psycopg2
import os

app = FastAPI()

# 🔥 Railway ENV'den al
DB_URL = os.getenv("DATABASE_URL")


def get_conn():
    if not DB_URL:
        raise Exception("DATABASE_URL bulunamadı")

    return psycopg2.connect(
        DB_URL,
        sslmode="require"
    )


@app.get("/")
def home():
    return {"status": "running"}


# 🔍 DEBUG (çok önemli)
@app.get("/debug")
def debug():
    return {
        "db_url_exists": DB_URL is not None,
        "db_url_preview": DB_URL[:40] if DB_URL else None
    }


# ✅ DB test
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


# ✅ MAÇLARI GETİR
@app.get("/matches")
def get_matches():

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT 
                match_id,
                home_team,
                away_team,
                ft_home,
                ft_away,
                league,
                date
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
                "score": f"{r[3]}-{r[4]}" if r[3] is not None else None,
                "league": r[5],
                "date": str(r[6])
            })

        return data

    except Exception as e:
        return {"error": str(e)}
