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
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ENV
ADMIN_USER = os.getenv("ADMIN_USER")
ADMIN_PASS_HASH = os.getenv("ADMIN_PASS_HASH")
SECRET_KEY = os.getenv("SECRET_KEY")

if not ADMIN_USER or not ADMIN_PASS_HASH or not SECRET_KEY:
    raise Exception("ENV eksik!")

# =========================
# 🔑 PASSWORD VERIFY
# =========================
def verify_password(plain, hashed):
    return pwd_context.verify(plain, hashed)

# =========================
# 🔑 TOKEN CREATE
# =========================
def create_token(username: str, role: str):
    return jwt.encode(
        {
            "user": username,
            "role": role,
            "exp": datetime.utcnow() + timedelta(hours=12)
        },
        SECRET_KEY,
        algorithm="HS256"
    )

# =========================
# 🔐 TOKEN DECODE
# =========================
def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload
    except:
        raise HTTPException(status_code=401, detail="Invalid token")

# =========================
# 🔐 ADMIN CHECK
# =========================
def require_admin(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return user

# =========================
# 🔑 LOGIN
# =========================
class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/login", tags=["Auth"])
def login(data: LoginRequest):

    if data.username != ADMIN_USER:
        raise HTTPException(status_code=401, detail="Wrong username")

    if not verify_password(data.password, ADMIN_PASS_HASH):
        raise HTTPException(status_code=401, detail="Wrong password")

    token = create_token(data.username, "admin")

    return {"access_token": token}

# =========================
# 🌍 PUBLIC ENDPOINT
# =========================
@app.get("/public", tags=["Public"])
def public():
    return {"msg": "Herkese açık"}

# =========================
# 🔐 AUTH REQUIRED
# =========================
@app.get("/profile", tags=["User"])
def profile(user=Depends(get_current_user)):
    return {
        "user": user["user"],
        "role": user["role"]
    }

# =========================
# 🔒 ADMIN ONLY
# =========================
@app.get("/admin/dashboard", tags=["Admin"])
def admin_dashboard(user=Depends(require_admin)):
    return {"msg": "Admin paneli"}
