import psycopg2
import os

def get_conn():
    url = os.environ.get("DATABASE_URL")

    return psycopg2.connect(
        url,
        sslmode="require",
        connect_timeout=10
    )
