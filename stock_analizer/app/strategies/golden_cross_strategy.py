
"""
Golden Cross Strategy Module
--------------------------------
This module implements the Golden Cross stock screening strategy for S&P 500 stocks.
It fetches historical price data, detects golden cross events (50-day MA crossing above 200-day MA),
and stores the results in the database. Can be run as a standalone script or imported as a module.

Key Functions:
- detect_golden_cross: Detects golden cross in a DataFrame of price data.
- screen_golden_cross: Async function to check a single ticker for golden cross.
- golden_cross_strategy: Async function to screen all S&P 500 tickers.
- run_and_store_golden_cross: Runs the strategy and stores results in the database.
"""

import json
import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from app.db import SessionLocal, GoldenCrossData

# app/strategies/golden_cross_strategy.py

import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from app.services.polygon import get_polygon_data  # ✅ assumes existing shared function for polygon data
from app.services.sp500 import get_sp500_tickers  # ✅ shared function to fetch S&P500



def detect_golden_cross(df: pd.DataFrame) -> bool:
    """
    Detects if a golden cross (50-day MA crosses above 200-day MA) occurred in the last 7 days.

    Args:
        df (pd.DataFrame): DataFrame with a 'close' column and datetime index.

    Returns:
        bool: True if a golden cross was detected recently, False otherwise.
    """
    if df is None or len(df) < 200:
        return False

    df["MA50"] = df["close"].rolling(window=50).mean()
    df["MA200"] = df["close"].rolling(window=200).mean()
    df.dropna(subset=["MA50", "MA200"], inplace=True)

    df["Signal"] = (df["MA50"] > df["MA200"]).astype(int)
    df["Cross"] = df["Signal"].diff()

    cutoff = datetime.now() - timedelta(days=7)
    recent = df[(df.index >= cutoff) & (df["Cross"] == 1)]

    return not recent.empty



async def screen_golden_cross(ticker, session):
    """
    Checks if a given ticker has a recent golden cross event.

    Args:
        ticker (str): Stock ticker symbol.
        session (aiohttp.ClientSession): HTTP session for API requests.

    Returns:
        str or None: Ticker if golden cross detected, else None.
    """
    try:
        df = await get_polygon_data(ticker, session)
        if df is None:
            return None
        if detect_golden_cross(df):
            return ticker
        return None
    except Exception:
        return None



async def golden_cross_strategy(session):
    """
    Screens all S&P 500 tickers for recent golden cross events.

    Args:
        session (aiohttp.ClientSession): HTTP session for API requests.

    Returns:
        list: List of tickers with a recent golden cross.
    """
    tickers = await get_sp500_tickers()  # ✅ Correct

    async with asyncio.Semaphore(10):  # Limit concurrency
        async with aiohttp.ClientSession() as session:
            tasks = [screen_golden_cross(t, session) for t in tickers]
            results = await asyncio.gather(*tasks)
            return [t for t in results if t]



async def run_and_store_golden_cross():
    """
    Runs the golden cross strategy and stores the results in the database.
    Fetches S&P 500 tickers, checks each for a recent golden cross, and inserts results into GoldenCrossData table.
    """
    session = SessionLocal()
    try:
        tickers = await get_sp500_tickers()
        async with aiohttp.ClientSession() as aio_session:
            picks = await golden_cross_strategy(aio_session)
        now = datetime.now()
        today = now.date()
        time_now = now.time().replace(microsecond=0)
        for sym in picks:
            entry = GoldenCrossData(
                symbol=sym,
                data_date=today,
                data_time=time_now,
                data_json=json.dumps({"Ticker": sym}),
            )
            session.add(entry)
        session.commit()
        print(f"Inserted {len(picks)} Golden Cross results into DB for {today} {time_now}")
    except Exception as e:
        print(f"Error in run_and_store_golden_cross: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(run_and_store_golden_cross())