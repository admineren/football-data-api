from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"status": "running"}

@app.get("/matches")
def get_matches():
    return {"message": "matches endpoint ready"}
