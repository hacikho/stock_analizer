# app/main.py

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from aignitequant.app.api.routes import router
from aignitequant.config import settings
from fastapi.middleware.cors import CORSMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: immediately populate Redis so /market-pulse never
    # returns stale:true right after a cold deploy.
    # Celery Beat takes over every 30 seconds after this first fetch.
    for attempt in range(1, 4):
        try:
            from aignitequant.app.services.market_pulse import fetch_and_store_market_pulse
            print(f"Startup: fetching initial market pulse data (attempt {attempt}/3)...")
            stats = await fetch_and_store_market_pulse()
            print(f"Startup market pulse complete: {stats}")
            break
        except Exception as e:
            print(f"WARNING: Startup market pulse attempt {attempt} failed: {e}")
            if attempt < 3:
                await asyncio.sleep(3)

    yield  # app is running


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
