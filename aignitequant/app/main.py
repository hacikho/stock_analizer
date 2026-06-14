# app/main.py

import asyncio
import datetime
from contextlib import asynccontextmanager

import pytz
from fastapi import FastAPI
from aignitequant.app.api.routes import router
from aignitequant.config import settings
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

_EASTERN = pytz.timezone("US/Eastern")

# Market-pulse refresh cadence (seconds).
#   Open  : fast refresh so the live dashboard stays current.
#   Closed: slow refresh -- equities don't move, but we keep the Redis
#           snapshot warm (BTC trades 24/7) without burning compute/egress
#           overnight and on weekends.
_PULSE_INTERVAL_OPEN = 30
_PULSE_INTERVAL_CLOSED = 240


def _is_market_hours(now=None) -> bool:
    """True during US extended trading hours (4 AM-8 PM ET, Mon-Fri)."""
    now = now or datetime.datetime.now(_EASTERN)
    if now.weekday() >= 5:  # Saturday / Sunday
        return False
    return 4 <= now.hour < 20


async def _market_pulse_loop():
    """
    Background task: refresh the market-pulse snapshot on a cadence that
    backs off when US markets are closed.

    This is the SINGLE source of market-pulse refreshes. The duplicate
    Celery Beat task ('market-pulse-every-30sec') was removed so the fetch
    runs in exactly one place instead of twice every 30 seconds.
    """
    from aignitequant.app.services.market_pulse import fetch_and_store_market_pulse
    from aignitequant.app.services.events import publish_update
    while True:
        open_now = _is_market_hours()
        try:
            stats = await fetch_and_store_market_pulse()
            state = "open" if open_now else "closed"
            print(f"Market pulse refresh ({state}): {stats}")
            publish_update("market_pulse")
        except Exception as e:
            print(f"WARNING: Market pulse refresh failed: {e}")
        await asyncio.sleep(_PULSE_INTERVAL_OPEN if open_now else _PULSE_INTERVAL_CLOSED)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background loop: populates Redis immediately, then refreshes on a
    # market-hours-aware cadence. Self-contained in the API process -- this is
    # the only place market pulse is fetched (no Celery Beat duplicate).
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

# Compress responses > 500 bytes. Strategy endpoints return large JSON
# tables that the frontend re-fetches on every SSE update; gzip typically
# cuts that egress by 70-85% with no behavioural change.
app.add_middleware(GZipMiddleware, minimum_size=500)

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
