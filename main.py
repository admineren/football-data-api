from fastapi import FastAPI
import psycopg2
import os

app = FastAPI()

# 🔥 ENV'den al
DB_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(
        DB_URL,
        sslmode="require"
    )

@app.get("/")
def home():
    return {"status": "running"}

# DEBUG endpoint (çok önemli)
@app.get("/debug")
def debug():
    return {
        "db_url_exists": DB_URL is not None,
        "db_url_preview": DB_URL[:30] if DB_URL else None
    }

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
