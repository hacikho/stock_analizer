import json
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from app.db import SessionLocal, Stage2Data

"""
Mark Minervini's Stage 2 Strategy Screener
------------------------------------------
This script implements the technical screening rules for Mark Minervini's Stage 2 uptrend stocks.
It fetches historical price data for a ticker, calculates moving averages and Relative Strength, and checks if the stock meets all Stage 2 criteria.

Stage 2 Criteria (Minervini Trend Template):
--------------------------------------------
1. The current price is above both the 150-day and 200-day simple moving averages (SMA).
2. The 150-day SMA is above the 200-day SMA.
3. The 200-day SMA is trending up for at least 1 month (current value > value 5 trading days ago).
4. The 50-day SMA is above both the 150-day and 200-day SMAs.
5. The current price is above the 50-day SMA.
6. The current price is at least 30% above its 52-week low.
7. The current price is within at least 25% of its 52-week high.
8. The Relative Strength vs S&P 500 is ≥ 70 (indicating strong outperformance vs market).

How to use:
    - Import and use `check_trend_template(ticker)` in your code, or
    - Run this file directly to screen a list of tickers and print the qualified ones.
"""

import asyncio
from app.services.sp500 import get_sp500_tickers
from app.services.polygon import get_polygon_data
import aiohttp
import pandas as pd
import datetime
from dotenv import load_dotenv

def get_secret():
    env = os.getenv("ENVIRONMENT", "local").lower()

# Load API key from .env for local development
load_dotenv()

# Uses shared get_polygon_data from app.services.polygon (DB-first with API fallback)

def calculate_relative_strength(stock_prices, spy_prices, period=252):
    """
    Calculate a Relative Strength proxy vs S&P 500 (SPY) over the specified period.
    Returns a value between 0-100, where ≥70 indicates strong outperformance.

    NOTE: This is NOT the IBD Relative Strength Rating (which ranks a stock's
    12-month performance against all other stocks as a 1-99 percentile). This is
    a custom proxy that compares the stock's % return vs SPY's % return over the
    period and normalizes the ratio to a 0-100 scale. Results are directionally
    similar but not equivalent to IBD RS — treat the 70 threshold as approximate.
    """
    if len(stock_prices) < period or len(spy_prices) < period:
        return 0
    
    # Calculate percentage change over the period
    stock_change = (stock_prices.iloc[-1] / stock_prices.iloc[-period]) - 1
    spy_change = (spy_prices.iloc[-1] / spy_prices.iloc[-period]) - 1
    
    # Relative strength calculation
    if spy_change != 0:
        relative_performance = stock_change / spy_change
    else:
        relative_performance = 1
    
    # Convert to 0-100 scale where 50 is neutral, >70 is strong outperformance
    rs_rating = min(100, max(0, (relative_performance - 0.5) * 100 + 50))
    
    return rs_rating

async def get_spy_data(session):
    """
    Fetch SPY data for relative strength calculation.
    """
    return await get_polygon_data('SPY', session)

async def check_trend_template(ticker, spy_data=None):
    """
    Check if a ticker meets all Mark Minervini Stage 2 technical criteria.
    Now uses Relative Strength vs S&P 500 instead of RSI.
    Returns True if all conditions are met, False otherwise.
    """
    async with aiohttp.ClientSession() as session:
        # Fetch stock data
        hist = await get_polygon_data(ticker, session)
        if hist is None or hist.empty:
            return False
        
        # Fetch SPY data if not provided
        if spy_data is None:
            spy_data = await get_spy_data(session)
        if spy_data is None or spy_data.empty:
            return False
        
        # Calculate moving averages
        hist['50_MA'] = hist['close'].rolling(window=50).mean()
        hist['150_MA'] = hist['close'].rolling(window=150).mean()
        hist['200_MA'] = hist['close'].rolling(window=200).mean()
        
        # Calculate Relative Strength vs SPY
        rs_rating = 0
        if len(hist) >= 252 and len(spy_data) >= 252:
            # Align dates between stock and SPY data
            aligned_spy = spy_data.reindex(hist.index, method='ffill')
            rs_rating = calculate_relative_strength(hist['close'], aligned_spy['close'])
        
        latest = hist.iloc[-1]
        
        # Check if we have enough data
        if any(pd.isna([latest['50_MA'], latest['150_MA'], latest['200_MA']])):
            return False
        
        # 1. Current price above 150-day and 200-day SMA
        if not (latest['close'] > latest['150_MA'] and latest['close'] > latest['200_MA']):
            return False
        
        # 2. 150-day SMA above 200-day SMA
        if not (latest['150_MA'] > latest['200_MA']):
            return False
        
        # 3. 200-day SMA trending up (current > ~1 month ago, i.e. 20 trading days)
        if len(hist) < 20 or not (hist['200_MA'].iloc[-1] > hist['200_MA'].iloc[-20]):
            return False
        
        # 4. 50-day SMA above both 150-day and 200-day SMAs
        if not (latest['50_MA'] > latest['150_MA'] and latest['50_MA'] > latest['200_MA']):
            return False
        
        # 5. Current price above 50-day SMA
        if not (latest['close'] > latest['50_MA']):
            return False
        
        # 6. Current price at least 30% above 52-week low
        min_52_week = hist['close'].tail(252).min()
        if not (latest['close'] >= 1.3 * min_52_week):
            return False

        # 7. Current price within 25% of 52-week high
        max_52_week = hist['close'].tail(252).max()
        if not (latest['close'] >= 0.75 * max_52_week):
            return False
        
        # 8. NEW: Relative Strength vs S&P 500 ≥ 70
        if rs_rating < 70:
            return False
        
        return True

def screen_stage2_from_df(ticker: str, df: pd.DataFrame, spy_df: pd.DataFrame) -> dict | None:
    """
    Synchronous Stage 2 screen using pre-loaded DataFrames.
    Returns a result dict if the ticker qualifies, None otherwise.
    """
    try:
        if df is None or df.empty or len(df) < 200:
            return None

        df = df.copy()
        df["50_MA"] = df["close"].rolling(50).mean()
        df["150_MA"] = df["close"].rolling(150).mean()
        df["200_MA"] = df["close"].rolling(200).mean()
        df = df.dropna(subset=["50_MA", "150_MA", "200_MA"])
        if len(df) < 5:
            return None

        latest = df.iloc[-1]
        price = latest["close"]

        # Criteria 1-7
        if not (price > latest["150_MA"] and price > latest["200_MA"]):
            return None
        if not (latest["150_MA"] > latest["200_MA"]):
            return None
        if not (df["200_MA"].iloc[-1] > df["200_MA"].iloc[-20]):
            return None
        if not (latest["50_MA"] > latest["150_MA"] and latest["50_MA"] > latest["200_MA"]):
            return None
        if not (price > latest["50_MA"]):
            return None

        low_52w = df["close"].tail(252).min()
        high_52w = df["close"].tail(252).max()
        if not (price >= 1.3 * low_52w):
            return None
        if not (price >= 0.75 * high_52w):
            return None

        # Criteria 8: RS vs SPY
        rs_rating = 0
        if spy_df is not None and not spy_df.empty and len(df) >= 252 and len(spy_df) >= 252:
            aligned_spy = spy_df.reindex(df.index, method="ffill")
            rs_rating = calculate_relative_strength(df["close"], aligned_spy["close"])
        if rs_rating < 70:
            return None

        pct_from_high = round((price / high_52w - 1) * 100, 2)
        pct_from_low = round((price / low_52w - 1) * 100, 2)

        return {
            "symbol": ticker,
            "price": round(price, 2),
            "ma50": round(latest["50_MA"], 2),
            "ma150": round(latest["150_MA"], 2),
            "ma200": round(latest["200_MA"], 2),
            "rs_rating": round(rs_rating, 1),
            "52w_high": round(high_52w, 2),
            "52w_low": round(low_52w, 2),
            "pct_from_high": pct_from_high,
            "pct_from_low": pct_from_low,
        }
    except Exception as e:
        print(f"[Stage2] Error screening {ticker}: {e}")
        return None


async def run_and_store_stage2():
    """
    Runs Stage 2 screen against all S&P 500 tickers and stores results in the database.
    Uses DB batch read for speed; falls back to Polygon API for missing tickers.
    """
    from app.services.market_data import get_dataframe_from_db, get_multiple_dataframes_from_db
    import time

    print("🚀 Stage 2 Strategy - S&P 500 Scanner")
    print("=" * 60)

    tickers = await get_sp500_tickers()
    print(f"✅ Got {len(tickers)} tickers to scan")

    # Batch read from DB (includes SPY)
    all_tickers = tickers + ["SPY"]
    t0 = time.time()
    dfs = get_multiple_dataframes_from_db(all_tickers)
    db_hits = sum(1 for v in dfs.values() if v is not None and not v.empty)
    print(f"📂 DB batch read: {db_hits}/{len(all_tickers)} tickers in {time.time()-t0:.2f}s")

    spy_df = dfs.get("SPY")
    qualified = []
    api_fallback = []

    for ticker in tickers:
        df = dfs.get(ticker)
        if df is not None and not df.empty:
            result = screen_stage2_from_df(ticker, df, spy_df)
            if result:
                qualified.append(result)
        else:
            api_fallback.append(ticker)

    # API fallback for missing tickers
    if api_fallback:
        print(f"🌐 API fallback for {len(api_fallback)} tickers...")
        async with aiohttp.ClientSession() as session:
            if spy_df is None:
                spy_df = await get_polygon_data("SPY", session)
            tasks = [check_trend_template(t, spy_df) for t in api_fallback]
            results = await asyncio.gather(*tasks)
            # check_trend_template returns True/False; re-screen with basic data
            for ticker, passed in zip(api_fallback, results):
                if passed:
                    qualified.append({"symbol": ticker, "criteria": "Stage2"})

    now = datetime.datetime.now()
    today = now.date()
    time_now = now.time().replace(microsecond=0)

    db = SessionLocal()
    try:
        for item in qualified:
            entry = Stage2Data(
                symbol=item["symbol"],
                data_date=today,
                data_time=time_now,
                data_json=json.dumps(item),
            )
            db.add(entry)
        db.commit()
        print(f"💾 Saved {len(qualified)} Stage 2 candidates to DB")
    except Exception as e:
        db.rollback()
        print(f"[Stage2] DB error: {e}")
        raise
    finally:
        db.close()

    return qualified


# --- Script entry point for command-line use ---
if __name__ == "__main__":
    import sys
    async def main():
        tickers = sys.argv[1:]
        if not tickers:
            print("No tickers provided, fetching S&P 500 tickers...")
            tickers = await get_sp500_tickers()
        
        # Fetch SPY data once for all calculations
        async with aiohttp.ClientSession() as session:
            spy_data = await get_spy_data(session)
        
        qualified = []
        now = datetime.datetime.now()
        today = now.date()
        time_now = now.time().replace(microsecond=0)
        session_db = SessionLocal()
        
        for ticker in tickers:
            print(f"Screening {ticker}...")
            if await check_trend_template(ticker, spy_data):
                qualified.append(ticker)
                # Insert into DB
                entry = Stage2Data(
                    symbol=ticker,
                    data_date=today,
                    data_time=time_now,
                    data_json=json.dumps({
                        "symbol": ticker, 
                        "date": str(today),
                        "criteria": "Stage2_with_RelativeStrength"
                    })
                )
                session_db.add(entry)
        
        session_db.commit()
        session_db.close()
        print("Qualified Stage 2 stocks:", qualified)
        
    asyncio.run(main())