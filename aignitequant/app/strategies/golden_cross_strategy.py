
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
import time
from datetime import datetime, timedelta
from app.services.polygon import get_polygon_data  # ✅ assumes existing shared function for polygon data
from app.services.sp500 import get_sp500_tickers  # ✅ shared function to fetch S&P500
from app.services.market_data import get_dataframe_from_db, get_multiple_dataframes_from_db



def detect_golden_cross(df: pd.DataFrame, ticker: str = "") -> dict:
    """
    Detects if a golden cross (50-day MA crosses above 200-day MA) occurred in the last 7 trading days.

    Args:
        df (pd.DataFrame): DataFrame with a 'close' column and datetime index.
        ticker (str): Stock ticker symbol for debugging.

    Returns:
        dict: {"detected": bool, "date": str, "ma50": float, "ma200": float} or {"detected": False}
    """
    if df is None or len(df) < 200:
        return {"detected": False}

    # Create a copy to avoid modifying original DataFrame
    df = df.copy()
    
    # Calculate moving averages
    df["MA50"] = df["close"].rolling(window=50).mean()
    df["MA200"] = df["close"].rolling(window=200).mean()
    
    # Remove rows with NaN values
    df = df.dropna(subset=["MA50", "MA200"])
    
    if len(df) < 2:
        return {"detected": False}

    # Detect crossover: MA50 crosses above MA200
    # Previous day: MA50 < MA200 (strictly below, not equal)
    # Current day: MA50 > MA200
    # Add a minimum separation threshold to avoid noise (0.5% difference)
    df["MA50_below_prev"] = df["MA50"].shift(1) < df["MA200"].shift(1)
    df["MA50_above_now"] = df["MA50"] > df["MA200"]
    df["Separation"] = ((df["MA50"] - df["MA200"]) / df["MA200"] * 100)  # Percentage
    
    # Golden cross: was below, now above, with meaningful separation
    df["Golden_Cross"] = df["MA50_below_prev"] & df["MA50_above_now"] & (df["Separation"] > 0.1)

    # Check for golden cross in the last 7 trading days (not calendar days)
    last_7_trading_days = df.tail(7)
    recent_crosses = last_7_trading_days[last_7_trading_days["Golden_Cross"] == True]

    if len(recent_crosses) > 0:
        # Get the most recent cross
        cross_row = recent_crosses.iloc[-1]
        cross_date = cross_row.name
        return {
            "detected": True,
            "date": cross_date.strftime("%Y-%m-%d"),
            "ma50": round(cross_row["MA50"], 2),
            "ma200": round(cross_row["MA200"], 2),
            "separation_pct": round(cross_row["Separation"], 2)
        }
    
    return {"detected": False}



def screen_golden_cross_from_df(ticker: str, df: pd.DataFrame):
    """
    Checks if a given ticker has a recent golden cross event using a pre-loaded DataFrame.
    """
    try:
        if df is None or df.empty:
            return None
        result = detect_golden_cross(df, ticker)
        if result["detected"]:
            return (ticker, result)
        return None
    except Exception as e:
        print(f"❌ Error screening {ticker}: {e}")
        return None


async def screen_golden_cross(ticker, session):
    """
    Legacy async wrapper - checks if a given ticker has a recent golden cross event.
    """
    try:
        df = await get_polygon_data(ticker, session)
        if df is None:
            return None
        result = detect_golden_cross(df, ticker)
        if result["detected"]:
            return (ticker, result)
        return None
    except Exception as e:
        print(f"❌ Error screening {ticker}: {e}")
        return None



async def golden_cross_strategy(session=None):
    """
    Screens all S&P 500 tickers for recent golden cross events.
    Uses DB batch read for speed, falls back to API for missing tickers.
    """
    tickers = await get_sp500_tickers()

    # --- DB batch read ---
    t0 = time.time()
    dfs = get_multiple_dataframes_from_db(tickers)
    db_hits = sum(1 for v in dfs.values() if v is not None and not v.empty)
    print(f"📂 DB batch read: {db_hits}/{len(tickers)} tickers in {time.time()-t0:.2f}s")

    results = []
    api_fallback = []

    # Screen from DB
    for t in tickers:
        df = dfs.get(t)
        if df is not None and not df.empty:
            r = screen_golden_cross_from_df(t, df)
            if r:
                results.append(r)
        else:
            api_fallback.append(t)

    # API fallback for missing
    if api_fallback and session:
        print(f"🌐 API fallback for {len(api_fallback)} tickers...")
        tasks = [screen_golden_cross(t, session) for t in api_fallback]
        api_results = await asyncio.gather(*tasks)
        results.extend([r for r in api_results if r is not None])

    return results



async def run_and_store_golden_cross():
    """
    Runs the golden cross strategy and stores the results in the database.
    Fetches S&P 500 tickers, checks each for a recent golden cross, and inserts results into GoldenCrossData table.
    """
    print("🚀 Golden Cross Strategy - S&P 500 Scanner")
    print("=" * 60)
    print("Scanning for 50-day MA crossing above 200-day MA...")
    print("Looking for crosses within the last 7 trading days")
    print("=" * 60)
    
    session = SessionLocal()
    try:
        print("\n📊 Fetching S&P 500 tickers...")
        tickers = await get_sp500_tickers()
        print(f"✅ Got {len(tickers)} tickers to scan\n")
        
        print("🔍 Analyzing stocks for Golden Cross patterns...")
        t_start = time.time()
        async with aiohttp.ClientSession() as aio_session:
            picks = await golden_cross_strategy(aio_session)
        print(f"Total screening took {time.time()-t_start:.2f}s")
        
        now = datetime.now()
        today = now.date()
        time_now = now.time().replace(microsecond=0)
        
        print(f"\n{'='*60}")
        print(f"✨ GOLDEN CROSS RESULTS - {today} {time_now}")
        print(f"{'='*60}")
        
        if picks:
            print(f"\n🎯 Found {len(picks)} stocks with Golden Cross signals:\n")
            for idx, (sym, details) in enumerate(picks, 1):
                cross_date = details.get("date", "N/A")
                ma50 = details.get("ma50", 0)
                ma200 = details.get("ma200", 0)
                sep_pct = details.get("separation_pct", 0)
                
                print(f"   {idx:2d}. 📈 {sym:6s} | Cross: {cross_date} | MA50: ${ma50:7.2f} | MA200: ${ma200:7.2f} | Sep: {sep_pct:+.2f}%")
                
                entry = GoldenCrossData(
                    symbol=sym,
                    data_date=today,
                    data_time=time_now,
                    data_json=json.dumps({
                        "Ticker": sym, 
                        "CrossDate": cross_date,
                        "MA50": ma50,
                        "MA200": ma200,
                        "Separation_pct": sep_pct
                    }),
                )
                session.add(entry)
            
            session.commit()
            print(f"\n💾 Database: Saved {len(picks)} Golden Cross signals")
            print(f"{'='*60}")
            print(f"✅ Analysis complete - {len(picks)} candidates identified")
        else:
            print("\n⚠️  No Golden Cross signals found")
            print("   The 50-day MA has not crossed above the 200-day MA")
            print("   for any S&P 500 stocks in the last 7 trading days")
        
        print(f"{'='*60}\n")
        
        return picks
        
    except Exception as e:
        print(f"\n❌ Error in run_and_store_golden_cross: {e}")
        session.rollback()
        return []
    finally:
        session.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(run_and_store_golden_cross())