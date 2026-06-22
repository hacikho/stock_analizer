# app/main.py

import asyncio
import datetime
from contextlib import asynccontextmanager

import pytz
from fastapi import FastAPI
from aignitequant.app.api.routes import router
from aignitequant.app.middleware import ETagMiddleware
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
    """
    True during US extended trading hours (4 AM-8 PM ET, Mon-Fri),
    excluding US market holidays. Delegates to the shared market calendar
    so weekends, off-hours, AND holidays all back the pulse loop off to the
    slow 'closed' cadence.
    """
    from aignitequant.market_calendar import is_market_open
    return is_market_open(now)


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
            # Only notify connected dashboards while the market is open. When
            # closed, equities don't move, so publishing an SSE event every
            # cycle just makes every open browser tab re-fetch the strategy
            # tables 24/7 -- a major driver of off-hours egress. The Redis
            # snapshot is still refreshed on the slow cadence above, so a tab
            # that loads while closed still gets fresh data on its first fetch.
            if open_now:
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

# Conditional caching: add an ETag to every GET/HEAD response and reply
# 304 Not Modified when the dashboard re-polls unchanged data. This is the
# main egress fix -- the frontend re-fetches the same endpoints on every SSE
# update, and 304s turn those repeat downloads into a few header bytes.
# Added BEFORE GZip so the response stack is CORS -> GZip -> ETag -> app:
# the ETag is computed over the stable uncompressed body, and a 304
# short-circuits before any compression work.
app.add_middleware(ETagMiddleware)

# Compress responses > 500 bytes. Strategy endpoints return large JSON
# tables that the frontend re-fetches on every SSE update; gzip typically
# cuts that egress by 70-85% with no behavioural change.
app.add_middleware(GZipMiddleware, minimum_size=500)

# Build CORS origins: defaults + production frontend URL if set
cors_origins = list(settings.CORS_ORIGINS)
if settings.FRONTEND_URL:
    cors_origins.extend(u.strip() for u in settings.FRONTEND_URL.split(",") if u.strip())

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)
