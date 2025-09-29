import json
import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from app.db import SessionLocal, BoraData
from app.services.sp500 import get_sp500_tickers


"""
Bora Strategy: Trend-Following Stock Screener
------------------------------------------------
This module implements a trend-following stock screening strategy based on the following rules:

1. The stock's price must be above its 200-day simple moving average (SMA_200).
2. The 21-day exponential moving average (EMA_21) must be above the 50-day EMA (EMA_50).
3. The EMA_21 must be trending up, as measured by slope, percent change, or strict monotonicity over a lookback window.

The strategy can be run as a standalone script (for scheduled jobs) or imported as a module.
Results are stored in the BoraData table in the database for later retrieval via API.
"""

import pandas as pd
import numpy as np
import aiohttp
import asyncio
from app.services.polygon import get_polygon_data

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add SMA_200, EMA_21, and EMA_50 columns to the input DataFrame.
    Args:
        df: DataFrame with at least a 'close' column.
    Returns:
        DataFrame with new indicator columns.
    """
    df = df.copy()
    df["SMA_200"] = df["close"].rolling(window=200).mean()
    df["EMA_21"]  = df["close"].ewm(span=21, adjust=False).mean()
    df["EMA_50"]  = df["close"].ewm(span=50, adjust=False).mean()
    return df

def ema21_trend_ok(df: pd.DataFrame, lookback=10, method="slope", slope_thresh=0.0, pct_thresh=1.0) -> bool:
    """
    Check if the EMA_21 is trending up over the lookback window.
    Args:
        df: DataFrame with 'EMA_21' column.
        lookback: Number of days to check.
        method: 'slope', 'pct', or 'strict'.
        slope_thresh: Minimum slope (for 'slope' method).
        pct_thresh: Minimum percent increase (for 'pct' method).
    Returns:
        True if trend criteria are met, False otherwise.
    """
    recent = df["EMA_21"].dropna().iloc[-lookback:]
    if len(recent) < lookback:
        return False

    if method == "slope":
        x = np.arange(lookback)
        y = recent.values
        slope, _ = np.polyfit(x, y, 1)
        return slope > slope_thresh

    elif method == "pct":
        pct_change = (recent.iloc[-1] / recent.iloc[0] - 1) * 100
        return pct_change > pct_thresh

    else:  # strict
        return all(x < y for x, y in zip(recent, recent[1:]))

async def scan_single_symbol(sym, session, method, slope_thresh, pct_thresh, lookback):
    """
    Screen a single symbol for Bora strategy criteria.
    Args:
        sym: Stock symbol.
        session: aiohttp session.
        method, slope_thresh, pct_thresh, lookback: Trend parameters.
    Returns:
        Symbol if it passes all filters, else None.
    """
    df = await get_polygon_data(sym, session)
    if df is None or len(df) < 200:
        return None

    df = compute_indicators(df)
    try:
        price_last = df["close"].iloc[-1]
        sma200_last = df["SMA_200"].iloc[-1]
        ema21_last = df["EMA_21"].iloc[-1]
        ema50_last = df["EMA_50"].iloc[-1]
    except Exception:
        return None

    if price_last <= sma200_last:
        return None
    if ema21_last <= ema50_last:
        return None
    if not ema21_trend_ok(df, lookback=lookback, method=method, slope_thresh=slope_thresh, pct_thresh=pct_thresh):
        return None

    return sym

async def scan_symbols(symbols, ema21_method="slope", slope_thresh=0.0, pct_thresh=1.0, lookback=10):
    """
    Run Bora strategy screen on a list of symbols.
    Args:
        symbols: List of stock symbols.
        ema21_method, slope_thresh, pct_thresh, lookback: Trend parameters.
    Returns:
        List of symbols that pass the Bora strategy.
    """
    picks = []
    async with aiohttp.ClientSession() as session:
        tasks = [
            scan_single_symbol(sym, session, ema21_method, slope_thresh, pct_thresh, lookback)
            for sym in symbols
        ]
        results = await asyncio.gather(*tasks)
        picks = [r for r in results if r]
    return picks
async def run_and_store_bora():
    """
    Run Bora strategy on S&P 500 tickers and store results in the database.
    """
    picks = []
    async with aiohttp.ClientSession() as session:
        tasks = [
            scan_single_symbol(sym, session, ema21_method, slope_thresh, pct_thresh, lookback)
            for sym in symbols
        ]
        results = await asyncio.gather(*tasks)
        picks = [r for r in results if r]
    return picks


async def run_and_store_bora():
    session = SessionLocal()
    try:
        tickers = await get_sp500_tickers()
        # Set default parameters for scan_symbols
        ema21_method = "slope"
        slope_thresh = 0.0
        pct_thresh = 1.0
        lookback = 10
        picks = await scan_symbols(tickers, ema21_method=ema21_method, slope_thresh=slope_thresh, pct_thresh=pct_thresh, lookback=lookback)
        now = datetime.datetime.now()
        today = now.date()
        time_now = now.time().replace(microsecond=0)
        for sym in picks:
            entry = BoraData(
                symbol=sym,
                data_date=today,
                data_time=time_now,
                data_json=json.dumps({"Ticker": sym}),
            )
            session.add(entry)
        session.commit()
        print(f"Inserted {len(picks)} BORA results into DB for {today} {time_now}")
    except Exception as e:
        print(f"Error in run_and_store_bora: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    """
    Entry point for running the Bora strategy as a script.
    Fetches S&P 500 tickers, runs the screen, and saves results to the database.
    """
    import asyncio
    asyncio.run(run_and_store_bora())