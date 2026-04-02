from fastapi import FastAPI
import psycopg2

app = FastAPI()

# 🔥 BURAYA SENİN CONNECTION STRING
DB_URL = "postgresql://postgres:Erenyalcin18..@db.ttmrttxmnfljcmtheqhv.supabase.co:5432/postgres"


def get_conn():
    return psycopg2.connect(
        host="db.ttmrttxmnfljcmtheqhv.supabase.co",
        database="postgres",
        user="postgres",
        password="SIFRE",
        port=5432,
        sslmode="require"
    )


@app.get("/")
def home():
    return {"status": "running"}

@app.get("/test-db")
def test_db():
    try:
        conn = get_conn()
        conn.close()
        return {"db": "connected"}
    except Exception as e:
        return {"error": str(e)}


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
