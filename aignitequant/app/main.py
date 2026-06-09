# app/main.py

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from aignitequant.app.api.routes import router
from aignitequant.config import settings
from fastapi.middleware.cors import CORSMiddleware


async def _market_pulse_loop():
    """Background task: fetch market pulse every 30s, forever."""
    from aignitequant.app.services.market_pulse import fetch_and_store_market_pulse
    while True:
        try:
            stats = await fetch_and_store_market_pulse()
            print(f"Market pulse refresh: {stats}")
        except Exception as e:
            print(f"WARNING: Market pulse refresh failed: {e}")
        await asyncio.sleep(30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background loop: populates Redis immediately, then every 30s.
    # Self-contained in the API process — no Celery Beat needed for this task.
    task = asyncio.create_task(_market_pulse_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Stock Market Analysis API",
    version="1.0.0",
    lifespan=lifespan,
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
