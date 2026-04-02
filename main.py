from fastapi import FastAPI
import psycopg2
import os

app = FastAPI()

DB_URL = os.environ.get("DATABASE_URL")

def get_conn():
    return psycopg2.connect(
        DB_URL,
        sslmode="require"
    )

@app.get("/debug")
def debug():
    return {
        "has_db_url": DB_URL is not None,
        "db_url_preview": DB_URL[:20] if DB_URL else None
    }

@app.get("/test-db")
def test_db():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug-full")
def debug_full():
    return dict(os.environ)
