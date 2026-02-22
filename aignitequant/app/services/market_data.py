"""
Centralized Market Data Service
================================
Fetches OHLCV data from Polygon.io and stores it in the shared `market_data` table.
Strategies read from the DB instead of calling the API directly.

Architecture:
    [Celery Beat] → fetch_all_market_data() → Polygon.io API → market_data table
    [Strategies]  → get_dataframe_from_db(ticker) → reads from market_data table

Benefits:
    - 1 fetch job per interval vs. N strategies × 500 tickers each
    - Strategy execution drops from ~90s (API) to ~2s (DB read)
    - No Polygon rate-limit issues across concurrent strategies
    - All strategies use same data snapshot (consistency)
"""

import asyncio
import datetime
import os
import time
from typing import Dict, List, Optional

import aiohttp
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from aignitequant.app.db import SessionLocal, MarketData, MarketDataMeta
from aignitequant.app.services.sp500 import get_sp500_tickers

load_dotenv()
API_KEY = os.getenv("API_KEY")


# ============================================================
# WRITE SIDE — Called by Celery task every 10 minutes
# ============================================================

async def _fetch_ticker_data(
    ticker: str,
    session: aiohttp.ClientSession,
    days: int = 730,
) -> Optional[pd.DataFrame]:
    """
    Fetch daily OHLCV bars for a single ticker from Polygon.io.
    Returns DataFrame or None on failure.
    """
    today = datetime.date.today()
    start = today - datetime.timedelta(days=days)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{today}"
    params = {"apiKey": API_KEY, "limit": 50000}

    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with session.get(url, params=params, timeout=timeout) as resp:
            if resp.status != 200:
                print(f"⚠️  Polygon HTTP {resp.status} for {ticker}")
                return None
            data = await resp.json()
            results = data.get("results")
            if not results:
                return None

            df = pd.DataFrame(results)
            df["timestamp"] = pd.to_datetime(df["t"], unit="ms")
            df["trade_date"] = df["timestamp"].dt.date
            df["symbol"] = ticker
            df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}, inplace=True)
            return df[["symbol", "trade_date", "open", "high", "low", "close", "volume"]]
    except Exception as e:
        print(f"❌ Error fetching {ticker}: {e}")
        return None


async def fetch_all_market_data(
    batch_size: int = 5,
    delay: float = 1.0,
    extra_tickers: Optional[List[str]] = None,
) -> Dict[str, int]:
    """
    Master fetch job — pulls OHLCV data for all S&P 500 tickers (+ extras)
    from Polygon.io and upserts into the `market_data` table.

    Args:
        batch_size: Number of concurrent API requests per batch.
        delay: Seconds to pause between batches (rate-limit safety).
        extra_tickers: Additional tickers outside S&P 500 (e.g. SPY, QQQ, TQQQ, VIX).

    Returns:
        Dict with stats: {"tickers_fetched", "rows_upserted", "errors", "elapsed_sec"}.
    """
    t0 = time.time()

    # Build ticker list
    sp500 = await get_sp500_tickers()
    tickers = list(set(sp500 + (extra_tickers or ["SPY", "QQQ", "TQQQ"])))
    tickers.sort()

    print(f"📊 Market Data Fetch: {len(tickers)} tickers, batch_size={batch_size}")

    total_rows = 0
    errors = 0
    fetched = 0

    async with aiohttp.ClientSession() as http_session:
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i: i + batch_size]
            tasks = [_fetch_ticker_data(t, http_session) for t in batch]
            results = await asyncio.gather(*tasks)

            for df in results:
                if df is not None and not df.empty:
                    rows = _upsert_market_data(df)
                    total_rows += rows
                    fetched += 1
                else:
                    errors += 1

            # Progress log every 50 tickers
            done = min(i + batch_size, len(tickers))
            if done % 50 < batch_size:
                print(f"   ... {done}/{len(tickers)} tickers processed")

            if i + batch_size < len(tickers):
                await asyncio.sleep(delay)

    # Update metadata timestamp
    _update_meta("last_fetch_utc", datetime.datetime.utcnow().isoformat())
    _update_meta("last_fetch_tickers", str(fetched))
    _update_meta("last_fetch_rows", str(total_rows))

    elapsed = round(time.time() - t0, 1)
    stats = {
        "tickers_fetched": fetched,
        "rows_upserted": total_rows,
        "errors": errors,
        "elapsed_sec": elapsed,
    }
    print(f"✅ Market Data Fetch complete: {stats}")
    return stats


def _upsert_market_data(df: pd.DataFrame) -> int:
    """
    Insert or update OHLCV rows into market_data table.
    Uses INSERT OR REPLACE (SQLite) for upsert semantics.
    """
    db = SessionLocal()
    try:
        rows = 0
        for _, row in df.iterrows():
            # Use raw SQL for upsert (SQLite INSERT OR REPLACE)
            db.execute(
                text("""
                    INSERT INTO market_data (symbol, trade_date, open, high, low, close, volume)
                    VALUES (:symbol, :trade_date, :open, :high, :low, :close, :volume)
                    ON CONFLICT(symbol, trade_date) DO UPDATE SET
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume
                """),
                {
                    "symbol": row["symbol"],
                    "trade_date": str(row["trade_date"]),
                    "open": float(row["open"]) if pd.notna(row["open"]) else None,
                    "high": float(row["high"]) if pd.notna(row["high"]) else None,
                    "low": float(row["low"]) if pd.notna(row["low"]) else None,
                    "close": float(row["close"]),
                    "volume": float(row["volume"]) if pd.notna(row["volume"]) else None,
                },
            )
            rows += 1
        db.commit()
        return rows
    except Exception as e:
        db.rollback()
        print(f"❌ DB upsert error: {e}")
        return 0
    finally:
        db.close()


def _update_meta(key: str, value: str):
    """Insert or update a key in market_data_meta."""
    db = SessionLocal()
    try:
        db.execute(
            text("""
                INSERT INTO market_data_meta (key, value) VALUES (:key, :value)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """),
            {"key": key, "value": value},
        )
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"⚠️  Meta update error: {e}")
    finally:
        db.close()


# ============================================================
# READ SIDE — Called by strategies instead of get_polygon_data
# ============================================================

def get_dataframe_from_db(
    symbol: str,
    days: int = 730,
) -> Optional[pd.DataFrame]:
    """
    Read OHLCV data for a single ticker from the market_data table.
    
    Returns a DataFrame identical in shape to what `get_polygon_data()` returns:
        - Index: datetime (trade_date converted to Timestamp)
        - Columns: [open, high, low, close, volume]
        - Sorted ascending by date
    
    Args:
        symbol: Ticker symbol (e.g. "AAPL").
        days: Maximum number of calendar days of history to return.
    
    Returns:
        pd.DataFrame or None if no data found.
    """
    db = SessionLocal()
    try:
        cutoff = datetime.date.today() - datetime.timedelta(days=days)
        rows = (
            db.query(MarketData)
            .filter(MarketData.symbol == symbol, MarketData.trade_date >= cutoff)
            .order_by(MarketData.trade_date.asc())
            .all()
        )
        if not rows:
            return None

        records = [
            {
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
            }
            for r in rows
        ]
        df = pd.DataFrame(records, index=pd.to_datetime([r.trade_date for r in rows]))
        df.index.name = "timestamp"
        return df if not df.empty else None
    except Exception as e:
        print(f"❌ DB read error for {symbol}: {e}")
        return None
    finally:
        db.close()


def get_multiple_dataframes_from_db(
    symbols: List[str],
    days: int = 730,
) -> Dict[str, pd.DataFrame]:
    """
    Batch read OHLCV data for multiple tickers.
    Much faster than calling get_dataframe_from_db in a loop because it performs
    a single SQL query.

    Args:
        symbols: List of ticker symbols.
        days: Calendar days of history.

    Returns:
        Dict mapping symbol → DataFrame.  Missing symbols are excluded.
    """
    db = SessionLocal()
    try:
        cutoff = datetime.date.today() - datetime.timedelta(days=days)
        rows = (
            db.query(MarketData)
            .filter(MarketData.symbol.in_(symbols), MarketData.trade_date >= cutoff)
            .order_by(MarketData.symbol, MarketData.trade_date.asc())
            .all()
        )
        if not rows:
            return {}

        # Group rows by symbol
        grouped: Dict[str, list] = {}
        for r in rows:
            grouped.setdefault(r.symbol, []).append(r)

        result = {}
        for sym, sym_rows in grouped.items():
            records = [
                {"open": r.open, "high": r.high, "low": r.low, "close": r.close, "volume": r.volume}
                for r in sym_rows
            ]
            df = pd.DataFrame(records, index=pd.to_datetime([r.trade_date for r in sym_rows]))
            df.index.name = "timestamp"
            if not df.empty:
                result[sym] = df

        return result
    except Exception as e:
        print(f"❌ DB batch read error: {e}")
        return {}
    finally:
        db.close()


def get_last_fetch_time() -> Optional[str]:
    """Return the ISO timestamp of the last successful market data fetch, or None."""
    db = SessionLocal()
    try:
        row = db.query(MarketDataMeta).filter(MarketDataMeta.key == "last_fetch_utc").first()
        return row.value if row else None
    finally:
        db.close()


def is_market_data_fresh(max_age_minutes: int = 15) -> bool:
    """
    Check if market data was fetched recently enough to be considered fresh.
    Strategies can call this before running to guard against stale data.
    """
    ts = get_last_fetch_time()
    if not ts:
        return False
    try:
        last_fetch = datetime.datetime.fromisoformat(ts)
        age = (datetime.datetime.utcnow() - last_fetch).total_seconds() / 60
        return age <= max_age_minutes
    except Exception:
        return False


# ============================================================
# FALLBACK — wraps the old API call for strategies that need it
# ============================================================

async def get_polygon_data_with_db_fallback(
    ticker: str,
    session: aiohttp.ClientSession,
) -> Optional[pd.DataFrame]:
    """
    Try DB first, fall back to Polygon API if no data in DB.
    This allows a smooth migration — strategies can switch to this function
    and still work even if the fetch job hasn't run yet.
    """
    df = get_dataframe_from_db(ticker)
    if df is not None and len(df) >= 50:
        return df

    # Fallback to live API
    from aignitequant.app.services.polygon import get_polygon_data
    return await get_polygon_data(ticker, session)
