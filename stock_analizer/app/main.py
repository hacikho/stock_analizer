# app/main.py

from fastapi import FastAPI
from app.api.routes import router  # this is your APIRouter

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Stock Market Analysis API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite's dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)