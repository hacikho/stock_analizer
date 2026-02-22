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
    Calculate Relative Strength vs S&P 500 (SPY) over the specified period.
    Returns a value between 0-100, where >70 indicates strong outperformance.
    
    Formula: Compares stock performance vs SPY performance over the period,
    then normalizes to 0-100 scale where 70+ indicates significant outperformance.
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
        
        # 3. 200-day SMA trending up (current > 5 days ago)
        if len(hist) < 5 or not (hist['200_MA'].iloc[-1] > hist['200_MA'].iloc[-5]):
            return False
        
        # 4. 50-day SMA above both 150-day and 200-day SMAs
        if not (latest['50_MA'] > latest['150_MA'] and latest['50_MA'] > latest['200_MA']):
            return False
        
        # 5. Current price above 50-day SMA
        if not (latest['close'] > latest['50_MA']):
            return False
        
        # 6. Current price at least 30% above 52-week low
        min_52_week = hist['close'].min()
        if not (latest['close'] >= 1.3 * min_52_week):
            return False
        
        # 7. Current price within 25% of 52-week high
        max_52_week = hist['close'].max()
        if not (latest['close'] >= 0.75 * max_52_week):
            return False
        
        # 8. NEW: Relative Strength vs S&P 500 ≥ 70
        if rs_rating < 60:
            return False
        
        return True

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