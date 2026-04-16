import os
import jwt
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from passlib.context import CryptContext

app = FastAPI()

# 🔐 SECURITY
security = HTTPBearer()

# 🔐 PASSWORD CONTEXT
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ENV
ADMIN_USER = os.getenv("ADMIN_USER")
ADMIN_PASS_HASH = os.getenv("ADMIN_PASS_HASH")
SECRET_KEY = os.getenv("SECRET_KEY")


# =========================
# 🚨 ENV CHECK (ÇOK ÖNEMLİ)
# =========================
if not ADMIN_USER:
    raise Exception("ADMIN_USER missing")

if not ADMIN_PASS_HASH:
    raise Exception("ADMIN_PASS_HASH missing")

if not SECRET_KEY:
    raise Exception("SECRET_KEY missing")


# =========================
# 🔍 DEBUG PRINT (STARTUP)
# =========================
print("=== STARTUP DEBUG ===")
print("ADMIN_USER:", ADMIN_USER)
print("HASH:", ADMIN_PASS_HASH)
print("HASH LENGTH:", len(ADMIN_PASS_HASH))


# =========================
# 🔑 VERIFY FUNCTION
# =========================
def verify_password(plain, hashed):
    try:
        print("---- VERIFY DEBUG ----")
        print("INPUT:", plain)
        print("INPUT LEN:", len(plain))
        print("HASH:", hashed)
        print("HASH LEN:", len(hashed))

        result = pwd_context.verify(plain, hashed)

        print("VERIFY RESULT:", result)
        return result

    except Exception as e:
        print("VERIFY ERROR:", str(e))
        raise HTTPException(status_code=500, detail=f"Verify error: {str(e)}")


# =========================
# 🔑 LOGIN MODEL
# =========================
class LoginRequest(BaseModel):
    username: str
    password: str


# =========================
# 🔑 LOGIN ENDPOINT
# =========================
@app.post("/login")
def login(data: LoginRequest):

    print("=== LOGIN REQUEST ===")
    print("USERNAME:", data.username)
    print("PASSWORD:", data.password)

    if data.username != ADMIN_USER:
        raise HTTPException(status_code=401, detail="Wrong username")

    if not verify_password(data.password, ADMIN_PASS_HASH):
        raise HTTPException(status_code=401, detail="Wrong password")

    token = jwt.encode(
        {
            "user": data.username,
            "exp": datetime.utcnow() + timedelta(hours=12)
        },
        SECRET_KEY,
        algorithm="HS256"
    )

    return {"access_token": token}
