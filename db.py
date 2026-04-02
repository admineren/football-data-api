import psycopg2
import os

DB_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(
        DB_URL,
        sslmode="require",
        connect_timeout=10
    )
