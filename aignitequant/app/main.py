# app/main.py

from fastapi import FastAPI
from aignitequant.app.api.routes import router
from aignitequant.config import settings
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Stock Market Analysis API",
    version="1.0.0"
)

# Build CORS origins: defaults + production frontend URL if set
cors_origins = list(settings.CORS_ORIGINS)
if settings.FRONTEND_URL:
    cors_origins.append(settings.FRONTEND_URL)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)