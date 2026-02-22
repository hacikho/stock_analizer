"""
Intraday Market Data Service
=============================
Fetches 10-minute OHLCV bars from Polygon.io (Massive) covering:
    - Pre-market  : 4:00 AM – 9:29 AM ET
    - Regular      : 9:30 AM – 3:59 PM ET
    - After-hours  : 4:00 PM – 8:00 PM ET

Architecture:
    [Celery Beat] → fetch_intraday_data() → Polygon /range/10/minute/ → intraday_bars table
    [Strategies / API] → get_intraday_from_db(ticker) → reads from intraday_bars table

The task is scheduled every 10 minutes from 4:00 AM to 8:00 PM ET, Mon–Fri.
Older bars (> retention_days) are pruned automatically after each fetch.
"""

import asyncio
import datetime
import os
import time
from typing import Dict, List, Optional

import aiohttp
import pandas as pd
import pytz
from dotenv import load_dotenv
from sqlalchemy import text, and_

from aignitequant.app.db import SessionLocal, IntradayBar
from aignitequant.app.services.sp500 import get_sp500_tickers

load_dotenv()
API_KEY = os.getenv("API_KEY")

EASTERN = pytz.timezone("US/Eastern")

# Session boundaries in Eastern Time (hour, minute)
PRE_MARKET_START = (4, 0)
REGULAR_START = (9, 30)
POST_MARKET_START = (16, 0)
POST_MARKET_END = (20, 0)


def _classify_session(et_dt: datetime.datetime) -> str:
    """Classify a bar's Eastern-Time timestamp into a market session."""
    t = (et_dt.hour, et_dt.minute)
    if t < REGULAR_START:
        return "pre"
    elif t < POST_MARKET_START:
        return "regular"
    else:
        return "post"


# ============================================================
# WRITE SIDE — Called by Celery every 10 minutes (4 AM – 8 PM ET)
# ============================================================

async def _fetch_intraday_ticker(
    ticker: str,
    session: aiohttp.ClientSession,
    date_str: str,
) -> Optional[pd.DataFrame]:
    """
    Fetch 10-minute bars for a single ticker for one day from Polygon.io.
    
    Uses /v2/aggs/ticker/{ticker}/range/10/minute/{date}/{date}
    which returns all bars including pre-market and after-hours.
    
    Args:
        ticker: Stock symbol.
        session: aiohttp session.
        date_str: Date string YYYY-MM-DD.
    
    Returns:
        DataFrame with columns [symbol, bar_timestamp, open, high, low, close, volume, vwap, transactions]
        or None on failure.
    """
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/10/minute/{date_str}/{date_str}"
    params = {"apiKey": API_KEY, "limit": 50000, "sort": "asc"}

    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with session.get(url, params=params, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            results = data.get("results")
            if not results:
                return None

            df = pd.DataFrame(results)
            # 't' is Unix milliseconds UTC
            df["bar_timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True)
            df["bar_timestamp_et"] = df["bar_timestamp"].dt.tz_convert(EASTERN).dt.tz_localize(None)
            df["symbol"] = ticker
            df.rename(
                columns={
                    "o": "open",
                    "h": "high",
                    "l": "low",
                    "c": "close",
                    "v": "volume",
                    "vw": "vwap",
                    "n": "transactions",
                },
                inplace=True,
            )
            # Remove timezone info for SQLite storage (keep as naive UTC)
            df["bar_timestamp"] = df["bar_timestamp"].dt.tz_localize(None)

            # Classify session
            df["session"] = df["bar_timestamp_et"].apply(_classify_session)

            cols = ["symbol", "bar_timestamp", "bar_timestamp_et", "session",
                    "open", "high", "low", "close", "volume", "vwap", "transactions"]
            # Keep only columns that exist (vwap, transactions may be absent for some tickers)
            existing = [c for c in cols if c in df.columns]
            return df[existing]
    except Exception as e:
        print(f"Intraday fetch error for {ticker}: {e}")
        return None


def _upsert_intraday_bars(df: pd.DataFrame) -> int:
    """
    Insert or update 10-minute bars into the intraday_bars table.
    Uses INSERT ... ON CONFLICT ... DO UPDATE on (symbol, bar_timestamp).
    """
    db = SessionLocal()
    try:
        rows = 0
        for _, row in df.iterrows():
            db.execute(
                text("""
                    INSERT INTO intraday_bars
                        (symbol, bar_timestamp, bar_timestamp_et, session,
                         open, high, low, close, volume, vwap, transactions)
                    VALUES
                        (:symbol, :bar_timestamp, :bar_timestamp_et, :session,
                         :open, :high, :low, :close, :volume, :vwap, :transactions)
                    ON CONFLICT(symbol, bar_timestamp) DO UPDATE SET
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume,
                        vwap = excluded.vwap,
                        transactions = excluded.transactions,
                        session = excluded.session,
                        bar_timestamp_et = excluded.bar_timestamp_et
                """),
                {
                    "symbol": row["symbol"],
                    "bar_timestamp": str(row["bar_timestamp"]),
                    "bar_timestamp_et": str(row["bar_timestamp_et"]),
                    "session": row.get("session", "regular"),
                    "open": float(row["open"]) if pd.notna(row.get("open")) else None,
                    "high": float(row["high"]) if pd.notna(row.get("high")) else None,
                    "low": float(row["low"]) if pd.notna(row.get("low")) else None,
                    "close": float(row["close"]),
                    "volume": float(row["volume"]) if pd.notna(row.get("volume")) else None,
                    "vwap": float(row["vwap"]) if pd.notna(row.get("vwap")) else None,
                    "transactions": int(row["transactions"]) if pd.notna(row.get("transactions")) else None,
                },
            )
            rows += 1
        db.commit()
        return rows
    except Exception as e:
        db.rollback()
        print(f"Intraday upsert error: {e}")
        return 0
    finally:
        db.close()


def _prune_old_bars(retention_days: int = 5):
    """Delete intraday bars older than retention_days to keep DB lean."""
    db = SessionLocal()
    try:
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=retention_days)
        db.execute(
            text("DELETE FROM intraday_bars WHERE bar_timestamp < :cutoff"),
            {"cutoff": str(cutoff)},
        )
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"Intraday prune error: {e}")
    finally:
        db.close()


async def fetch_intraday_data(
    batch_size: int = 5,
    delay: float = 1.0,
    extra_tickers: Optional[List[str]] = None,
    retention_days: int = 5,
) -> Dict[str, int]:
    """
    Master intraday fetch job — pulls 10-minute bars for today
    for all S&P 500 tickers (+ extras) from Polygon.io and upserts
    into the intraday_bars table.

    Designed to run every 10 minutes from 4:00 AM to 8:00 PM ET.

    Args:
        batch_size: Concurrent API requests per batch.
        delay: Seconds between batches (rate-limit safety).
        extra_tickers: Additional tickers (e.g. SPY, QQQ, TQQQ).
        retention_days: Days of intraday data to keep (older pruned).

    Returns:
        Dict with stats: {"tickers_fetched", "rows_upserted", "errors", "elapsed_sec"}.
    """
    t0 = time.time()

    # Build ticker list
    sp500 = await get_sp500_tickers()
    tickers = list(set(sp500 + (extra_tickers or ["SPY", "QQQ", "TQQQ"])))
    tickers.sort()

    # Use today's date in ET
    now_et = datetime.datetime.now(EASTERN)
    date_str = now_et.strftime("%Y-%m-%d")
    session_label = _classify_session(now_et)

    print(f"Intraday Fetch: {len(tickers)} tickers, date={date_str}, "
          f"session={session_label}, batch_size={batch_size}")

    total_rows = 0
    errors = 0
    fetched = 0

    async with aiohttp.ClientSession() as http_session:
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i : i + batch_size]
            tasks = [_fetch_intraday_ticker(t, http_session, date_str) for t in batch]
            results = await asyncio.gather(*tasks)

            for df in results:
                if df is not None and not df.empty:
                    rows = _upsert_intraday_bars(df)
                    total_rows += rows
                    fetched += 1
                else:
                    errors += 1

            done = min(i + batch_size, len(tickers))
            if done % 100 < batch_size:
                print(f"   ... {done}/{len(tickers)} tickers processed (intraday)")

            if i + batch_size < len(tickers):
                await asyncio.sleep(delay)

    # Prune old data
    _prune_old_bars(retention_days)

    # Update metadata
    from aignitequant.app.services.market_data import _update_meta
    _update_meta("last_intraday_fetch_utc", datetime.datetime.utcnow().isoformat())
    _update_meta("last_intraday_session", session_label)

    elapsed = round(time.time() - t0, 1)
    stats = {
        "tickers_fetched": fetched,
        "rows_upserted": total_rows,
        "errors": errors,
        "session": session_label,
        "elapsed_sec": elapsed,
    }
    print(f"Intraday Fetch complete: {stats}")
    return stats


# ============================================================
# READ SIDE — Called by strategies / API endpoints
# ============================================================

def get_intraday_from_db(
    symbol: str,
    date: Optional[datetime.date] = None,
    session_filter: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """
    Read 10-minute intraday bars for a ticker from the DB.

    Args:
        symbol: Ticker symbol.
        date: Trading date (defaults to today ET).
        session_filter: Optional filter — 'pre', 'regular', 'post', or None for all.

    Returns:
        DataFrame with columns [open, high, low, close, volume, vwap, transactions, session]
        indexed by bar_timestamp_et (Eastern Time), or None if no data.
    """
    db = SessionLocal()
    try:
        if date is None:
            date = datetime.datetime.now(EASTERN).date()

        # Build date range in UTC for the ET trading day (4 AM ET = ~8/9 AM UTC)
        day_start_et = EASTERN.localize(datetime.datetime.combine(date, datetime.time(4, 0)))
        day_end_et = EASTERN.localize(datetime.datetime.combine(date, datetime.time(20, 0)))
        day_start_utc = day_start_et.astimezone(pytz.utc).replace(tzinfo=None)
        day_end_utc = day_end_et.astimezone(pytz.utc).replace(tzinfo=None)

        query = (
            db.query(IntradayBar)
            .filter(
                IntradayBar.symbol == symbol,
                IntradayBar.bar_timestamp >= day_start_utc,
                IntradayBar.bar_timestamp <= day_end_utc,
            )
        )
        if session_filter:
            query = query.filter(IntradayBar.session == session_filter)

        rows = query.order_by(IntradayBar.bar_timestamp.asc()).all()
        if not rows:
            return None

        records = [
            {
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
                "vwap": r.vwap,
                "transactions": r.transactions,
                "session": r.session,
            }
            for r in rows
        ]
        df = pd.DataFrame(records, index=pd.to_datetime([r.bar_timestamp_et for r in rows]))
        df.index.name = "timestamp_et"
        return df if not df.empty else None
    except Exception as e:
        print(f"Intraday DB read error for {symbol}: {e}")
        return None
    finally:
        db.close()


def get_multiple_intraday_from_db(
    symbols: List[str],
    date: Optional[datetime.date] = None,
    session_filter: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Batch read intraday bars for multiple tickers (single SQL query).

    Args:
        symbols: List of ticker symbols.
        date: Trading date (defaults to today ET).
        session_filter: Optional session filter.

    Returns:
        Dict mapping symbol -> DataFrame.
    """
    db = SessionLocal()
    try:
        if date is None:
            date = datetime.datetime.now(EASTERN).date()

        day_start_et = EASTERN.localize(datetime.datetime.combine(date, datetime.time(4, 0)))
        day_end_et = EASTERN.localize(datetime.datetime.combine(date, datetime.time(20, 0)))
        day_start_utc = day_start_et.astimezone(pytz.utc).replace(tzinfo=None)
        day_end_utc = day_end_et.astimezone(pytz.utc).replace(tzinfo=None)

        query = (
            db.query(IntradayBar)
            .filter(
                IntradayBar.symbol.in_(symbols),
                IntradayBar.bar_timestamp >= day_start_utc,
                IntradayBar.bar_timestamp <= day_end_utc,
            )
        )
        if session_filter:
            query = query.filter(IntradayBar.session == session_filter)

        rows = query.order_by(IntradayBar.symbol, IntradayBar.bar_timestamp.asc()).all()
        if not rows:
            return {}

        grouped: Dict[str, list] = {}
        for r in rows:
            grouped.setdefault(r.symbol, []).append(r)

        result = {}
        for sym, sym_rows in grouped.items():
            records = [
                {
                    "open": r.open,
                    "high": r.high,
                    "low": r.low,
                    "close": r.close,
                    "volume": r.volume,
                    "vwap": r.vwap,
                    "transactions": r.transactions,
                    "session": r.session,
                }
                for r in sym_rows
            ]
            df = pd.DataFrame(records, index=pd.to_datetime([r.bar_timestamp_et for r in sym_rows]))
            df.index.name = "timestamp_et"
            if not df.empty:
                result[sym] = df

        return result
    except Exception as e:
        print(f"Intraday batch read error: {e}")
        return {}
    finally:
        db.close()


def get_intraday_summary(
    symbol: str,
    date: Optional[datetime.date] = None,
) -> Optional[Dict]:
    """
    Quick summary of intraday data availability for a ticker.
    Useful for API health checks and dashboards.
    
    Returns:
        Dict with bar counts per session, latest bar timestamp, total volume, etc.
    """
    df = get_intraday_from_db(symbol, date)
    if df is None:
        return None
    
    return {
        "symbol": symbol,
        "date": str(date or datetime.datetime.now(EASTERN).date()),
        "total_bars": len(df),
        "pre_market_bars": int((df["session"] == "pre").sum()),
        "regular_bars": int((df["session"] == "regular").sum()),
        "post_market_bars": int((df["session"] == "post").sum()),
        "latest_bar": str(df.index[-1]),
        "latest_close": float(df["close"].iloc[-1]),
        "total_volume": float(df["volume"].sum()) if df["volume"].notna().any() else 0,
        "day_high": float(df["high"].max()) if df["high"].notna().any() else None,
        "day_low": float(df["low"].min()) if df["low"].notna().any() else None,
    }
