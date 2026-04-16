import os
import asyncpg
import jwt
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from passlib.context import CryptContext

# RATE LIMIT
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi.responses import JSONResponse

app = FastAPI(
    title="Football Data API",
    version="2.0.0"
)

# =========================
# 🔐 SECURITY
# =========================

security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

SECRET_KEY = os.getenv("SECRET_KEY")
ADMIN_USER = os.getenv("ADMIN_USER")
ADMIN_PASS_HASH = os.getenv("ADMIN_PASS_HASH")
DATABASE_URL = os.getenv("DATABASE_URL")

if not SECRET_KEY:
    raise ValueError("SECRET_KEY missing")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL missing")

pool = None

# =========================
# 🚫 RATE LIMIT
# =========================

limiter = Limiter(key_func=get_remote_address)

@app.exception_handler(RateLimitExceeded)
def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Too many requests"}
    )

# =========================
# 🚀 DB STARTUP
# =========================

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5
    )

@app.on_event("shutdown")
async def shutdown():
    await pool.close()

# =========================
# 🔐 AUTH LOGIC
# =========================

def verify_password(plain, hashed):
    return pwd_context.verify(plain, hashed)


def create_token(username: str, role: str):
    return jwt.encode(
        {
            "sub": username,
            "role": role,
            "iat": datetime.utcnow(),
            "exp": datetime.utcnow() + timedelta(hours=12)
        },
        SECRET_KEY,
        algorithm="HS256"
    )


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    try:
        token = credentials.credentials

        payload = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=["HS256"]
        )

        if payload.get("sub") != ADMIN_USER:
            raise HTTPException(403, "Unauthorized user")

        return payload

    except:
        raise HTTPException(401, "Invalid token")


def require_admin(user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(403, "Admin only")
    return user


# =========================
# 🔑 LOGIN
# =========================

class LoginRequest(BaseModel):
    username: str
    password: str


@app.post("/login", tags=["Auth"])
@limiter.limit("5/minute")
def login(request: Request, data: LoginRequest):

    if data.username != ADMIN_USER:
        raise HTTPException(401, "Wrong credentials")

    if not verify_password(data.password, ADMIN_PASS_HASH):
        raise HTTPException(401, "Wrong credentials")

    token = create_token(data.username, "admin")

    return {"access_token": token}


# =========================
# 🧪 HEALTH
# =========================

@app.get("/", tags=["System"])
async def health():
    try:
        async with pool.acquire() as conn:
            await conn.fetch("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# =========================
# 🛠 ADMIN ENDPOINTS
# =========================

@app.get("/admin/tables", tags=["Admin"])
async def get_tables(user=Depends(require_admin)):

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)

    return {"tables": [r["table_name"] for r in rows]}


@app.get("/admin/indexes", tags=["Admin"])
async def get_indexes(user=Depends(require_admin)):

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
        """)

    return [
        {
            "name": r["indexname"],
            "definition": r["indexdef"]
        }
        for r in rows
    ]


@app.get("/admin/columns", tags=["Admin"])
async def get_columns(table: str, user=Depends(require_admin)):

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = $1
        """, table)

    return [
        {
            "column": r["column_name"],
            "type": r["data_type"]
        }
        for r in rows
    ]
