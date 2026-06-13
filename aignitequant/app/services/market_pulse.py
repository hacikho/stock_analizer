"""
Market Pulse Service  —  Design B (Redis-only)
================================================
Fetches live snapshots for 8 macro instruments every minute and stores
the result as a single JSON blob in Redis.  The API reads directly from
Redis (<1 ms), so the frontend can poll as fast as every 10-30 seconds
with virtually zero database load.

No DB table is used.  If the Celery task hasn't run yet (cold start or
outside market hours), the API returns the last cached blob until the
TTL expires (5 minutes), then returns an empty/stale indicator.

Instrument → Polygon ticker mapping
-------------------------------------
These mirror exactly what Yahoo Finance displays in its US Markets bar.

  S&P 500      → I:SPX    (CBOE S&P 500 index — Yahoo ^GSPC)
  NASDAQ       → I:COMP   (Nasdaq Composite — Yahoo ^IXIC)
  Dow 30       → I:DJI    (Dow Jones Industrial Average — Yahoo ^DJI)
  Russell 2000 → I:RUT    (Russell 2000 index — Yahoo ^RUT)
  VIX          → I:VIX    (CBOE Volatility Index — Yahoo ^VIX, spot, NOT VXX futures)
  Gold         → GLD      (ETF proxy; Yahoo quotes GC=F futures — needs Futures add-on)
  Bitcoin      → X:BTCUSD (Polygon crypto ticker — direct, matches Yahoo BTC-USD)
  Crude Oil    → USO      (ETF proxy; Yahoo quotes CL=F futures — needs Futures add-on)

The five equity indices (I:*) require the Polygon **Indices** add-on.
Gold and Crude Oil remain ETF proxies because spot/futures commodity
values are a separate Polygon Futures product; swap them to futures
front-month tickers if/when that add-on is enabled.

API strategy
------------
  - 5 indices (I:SPX, I:COMP, I:DJI, I:RUT, I:VIX): one snapshot call
    GET /v3/snapshot/indices?ticker.any_of=...
  - 2 ETFs (GLD, USO): one batch snapshot call
    GET /v2/snapshot/locale/us/markets/stocks/tickers
  - Bitcoin: GET /v2/aggs/ticker/X:BTCUSD/prev  (previous close)
             + GET /v2/aggs/ticker/X:BTCUSD/range/1/day/{today}/{today}
               (today's intraday OHLCV)
  Total: 3-4 API calls per minute.

Redis keys
----------
  market_pulse:snapshot   JSON blob, TTL = REDIS_TTL_SECONDS (default 5 min)
"""

import asyncio
import datetime
import json
import os
from typing import Optional

import aiohttp
import redis
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

# Redis connection — reuse the same URL Celery uses
# Railway sets REDIS_PRIVATE_URL (internal) or REDIS_URL (public).
# Fall back to CELERY_BROKER_URL for local dev, then localhost.
_REDIS_URL = (
    os.getenv("REDIS_PRIVATE_URL") or
    os.getenv("REDIS_URL") or
    os.getenv("CELERY_BROKER_URL") or
    "redis://localhost:6379/0"
)
_redis_host = _REDIS_URL.split("@")[-1] if "@" in _REDIS_URL else _REDIS_URL
print("Market pulse Redis URL: " + _redis_host)
_REDIS_KEY = "market_pulse:snapshot"
REDIS_TTL_SECONDS = 300  # 5 minutes — data considered stale after this

# Instrument registry — display order is preserved
INSTRUMENTS = [
    {"key": "SP500",       "label": "S&P 500",      "ticker": "I:SPX",    "type": "index"},
    {"key": "NASDAQ",      "label": "Nasdaq",       "ticker": "I:COMP",   "type": "index"},
    {"key": "DOW30",       "label": "Dow 30",       "ticker": "I:DJI",    "type": "index"},
    {"key": "RUSSELL2000", "label": "Russell 2000", "ticker": "I:RUT",    "type": "index"},
    {"key": "VIX",         "label": "VIX",          "ticker": "I:VIX",    "type": "index"},
    {"key": "GOLD",        "label": "Gold",         "ticker": "GLD",      "type": "stock"},
    {"key": "BITCOIN",     "label": "Bitcoin",      "ticker": "X:BTCUSD", "type": "crypto"},
    {"key": "CRUDE_OIL",   "label": "Crude Oil",    "ticker": "USO",      "type": "stock"},
]

_INDEX_TICKERS = [i["ticker"] for i in INSTRUMENTS if i["type"] == "index"]
_ETF_TICKERS = [i["ticker"] for i in INSTRUMENTS if i["type"] == "stock"]
_INSTRUMENT_MAP = {i["key"]: i for i in INSTRUMENTS}


# ============================================================
# Redis client factory (lazy singleton)
# ============================================================

_redis_client: Optional[redis.Redis] = None


def _get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(_REDIS_URL, decode_responses=True)
    return _redis_client


# ============================================================
# Polygon fetch helpers
# ============================================================

async def _fetch_index_snapshots(session: aiohttp.ClientSession) -> dict:
    """
    Single Polygon call for all index tickers (I:SPX, I:COMP, I:DJI, I:RUT, I:VIX).
    Requires the Polygon Indices add-on. Returns {ticker: result_dict}.
    """
    url = "https://api.polygon.io/v3/snapshot/indices"
    params = {"ticker.any_of": ",".join(_INDEX_TICKERS), "apiKey": API_KEY}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"⚠️  Market pulse index snapshot HTTP {resp.status}: {body[:200]}")
                return {}
            data = await resp.json()
            return {r["ticker"]: r for r in data.get("results", [])}
    except Exception as e:
        print(f"❌ Market pulse index fetch error: {e}")
        return {}


async def _fetch_etf_snapshots(session: aiohttp.ClientSession) -> dict:
    """Single Polygon call for the ETF proxies (GLD, USO). Returns {ticker: snapshot_dict}."""
    if not _ETF_TICKERS:
        return {}
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {"tickers": ",".join(_ETF_TICKERS), "apiKey": API_KEY}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                print(f"⚠️  Market pulse ETF snapshot HTTP {resp.status}")
                return {}
            data = await resp.json()
            return {t["ticker"]: t for t in data.get("tickers", [])}
    except Exception as e:
        print(f"❌ Market pulse ETF fetch error: {e}")
        return {}


async def _fetch_btc_data(session: aiohttp.ClientSession) -> Optional[dict]:
    """
    Fetch Bitcoin OHLCV.
    Uses today's daily bar for intraday OHLCV + prev bar for change calculation.
    """
    today = datetime.date.today().isoformat()

    async def _bar(ticker, date):
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date}/{date}"
        try:
            async with session.get(url, params={"adjusted": "true", "apiKey": API_KEY},
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return None
                d = await r.json()
                results = d.get("results", [])
                return results[-1] if results else None
        except Exception:
            return None

    async def _prev(ticker):
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
        try:
            async with session.get(url, params={"adjusted": "true", "apiKey": API_KEY},
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return None
                d = await r.json()
                results = d.get("results", [])
                return results[0] if results else None
        except Exception:
            return None

    today_bar, prev_bar = await asyncio.gather(_bar("X:BTCUSD", today), _prev("X:BTCUSD"))

    if not prev_bar:
        return None

    bar = today_bar if today_bar else prev_bar
    prev_close = prev_bar["c"]
    change = bar["c"] - prev_close
    change_pct = (change / prev_close * 100) if prev_close else 0.0

    return {
        "price":      bar["c"],
        "open":       bar.get("o"),
        "high":       bar.get("h"),
        "low":        bar.get("l"),
        "close":      bar["c"],
        "prev_close": prev_close,
        "change":     change,
        "change_pct": change_pct,
        "volume":     bar.get("v"),
    }


# ============================================================
# Main fetch + Redis write  (called by Celery task every minute)
# ============================================================

async def fetch_and_store_market_pulse() -> dict:
    """
    Fetch live snapshots for all 8 instruments and cache in Redis.

    Writes a single JSON blob to ``market_pulse:snapshot`` with a 5-minute TTL.
    The API endpoint reads this key directly — no DB involved.

    Returns:
        dict: {instruments_updated, errors, elapsed_ms}
    """
    import time
    t0 = time.time()
    now_iso = datetime.datetime.utcnow().isoformat() + "Z"
    errors = 0

    async with aiohttp.ClientSession() as session:
        index_snap, etf_snap, btc_data = await asyncio.gather(
            _fetch_index_snapshots(session),
            _fetch_etf_snapshots(session),
            _fetch_btc_data(session),
        )

    result = []

    for meta in INSTRUMENTS:
        # --- Index instruments (Polygon Indices add-on) ---
        if meta["type"] == "index":
            ticker = meta["ticker"]
            snap = index_snap.get(ticker)
            if not snap:
                errors += 1
                print(f"⚠️  No index snapshot for {ticker}")
                continue

            sess = snap.get("session", {}) or {}
            value = snap.get("value")
            prev_close = sess.get("previous_close")
            close = sess.get("close") or value

            result.append({
                "instrument": meta["key"],
                "label":      meta["label"],
                "ticker":     ticker,
                "price":      value if value is not None else close,
                "open":       sess.get("open"),
                "high":       sess.get("high"),
                "low":        sess.get("low"),
                "close":      close,
                "prev_close": prev_close,
                "change":     round(sess.get("change", 0.0) or 0.0, 4),
                "change_pct": round(sess.get("change_percent", 0.0) or 0.0, 4),
                "volume":     None,  # indices have no volume
            })

        # --- ETF proxy instruments (Gold, Crude Oil) ---
        elif meta["type"] == "stock":
            ticker = meta["ticker"]
            snap = etf_snap.get(ticker)
            if not snap:
                errors += 1
                print(f"⚠️  No snapshot for {ticker}")
                continue

            day  = snap.get("day", {})
            prev = snap.get("prevDay", {})
            prev_close = prev.get("c")
            close = day.get("c") or prev_close

            result.append({
                "instrument": meta["key"],
                "label":      meta["label"],
                "ticker":     ticker,
                "price":      close,
                "open":       day.get("o"),
                "high":       day.get("h"),
                "low":        day.get("l"),
                "close":      close,
                "prev_close": prev_close,
                "change":     round(snap.get("todaysChange", 0.0) or 0.0, 4),
                "change_pct": round(snap.get("todaysChangePerc", 0.0) or 0.0, 4),
                "volume":     day.get("v"),
            })

    # --- Bitcoin ---
    btc_meta = _INSTRUMENT_MAP["BITCOIN"]
    if btc_data:
        result.append({
            "instrument": "BITCOIN",
            "label":      btc_meta["label"],
            "ticker":     btc_meta["ticker"],
            "price":      btc_data["price"],
            "open":       btc_data.get("open"),
            "high":       btc_data.get("high"),
            "low":        btc_data.get("low"),
            "close":      btc_data["close"],
            "prev_close": btc_data.get("prev_close"),
            "change":     round(btc_data.get("change", 0.0) or 0.0, 4),
            "change_pct": round(btc_data.get("change_pct", 0.0) or 0.0, 4),
            "volume":     btc_data.get("volume"),
        })
    else:
        errors += 1
        print("⚠️  No BTC data returned")

    # Re-sort to canonical display order
    order = [i["key"] for i in INSTRUMENTS]
    result.sort(key=lambda x: order.index(x["instrument"]) if x["instrument"] in order else 99)

    # --- Write to Redis ---
    payload = json.dumps({"data": result, "last_updated": now_iso, "count": len(result)})
    try:
        r = _get_redis()
        r.set(_REDIS_KEY, payload, ex=REDIS_TTL_SECONDS)
    except Exception as e:
        print(f"❌ Redis write error: {e}")
        errors += 1

    elapsed_ms = round((time.time() - t0) * 1000)
    stats = {"instruments_updated": len(result), "errors": errors, "elapsed_ms": elapsed_ms}
    print(f"✅ Market pulse → Redis: {stats}")
    return stats


# ============================================================
# Read side  (called by the API endpoint)
# ============================================================

def get_market_pulse() -> dict:
    """
    Read the latest market pulse snapshot from Redis.

    Returns the full payload dict:
        {data: [...], last_updated: "...", count: 8}

    If Redis is empty or unavailable, returns:
        {data: [], last_updated: None, count: 0, stale: True}
    """
    try:
        r = _get_redis()
        raw = r.get(_REDIS_KEY)
        if raw:
            return json.loads(raw)
    except Exception as e:
        print(f"❌ Redis read error: {e}")

    return {"data": [], "last_updated": None, "count": 0, "stale": True}
