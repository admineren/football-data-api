from fastapi import FastAPI
import psycopg2
import os

app = FastAPI()


def get_conn():
    return psycopg2.connect(
        os.environ.get("DATABASE_URL"),
        sslmode="require",
        connect_timeout=10
    )


@app.get("/")
def home():
    return {"status": "running"}


@app.get("/debug")
def debug():
    return {"url": os.environ.get("DATABASE_URL")}


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
