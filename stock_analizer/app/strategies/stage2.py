import json
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from app.db import SessionLocal, Stage2Data


"""
Mark Minervini's Stage 2 Strategy Screener
------------------------------------------
This script implements the technical screening rules for Mark Minervini's Stage 2 uptrend stocks.
It fetches historical price data for a ticker, calculates moving averages and RSI, and checks if the stock meets all Stage 2 criteria.

Stage 2 Criteria (Minervini Trend Template):
--------------------------------------------
1. The current price is above both the 150-day and 200-day simple moving averages (SMA).
2. The 150-day SMA is above the 200-day SMA.
3. The 200-day SMA is trending up for at least 1 month (current value > value 5 trading days ago).
4. The 50-day SMA is above both the 150-day and 200-day SMAs.
5. The current price is above the 50-day SMA.
6. The current price is at least 30% above its 52-week low.
7. The current price is within at least 25% of its 52-week high.
8. The Relative Strength Index (RSI) is above 70 (indicating strong momentum).

How to use:
    - Import and use `check_trend_template(ticker)` in your code, or
    - Run this file directly to screen a list of tickers and print the qualified ones.
"""


import asyncio
from app.services.sp500 import get_sp500_tickers
import aiohttp
import pandas as pd
import datetime
from dotenv import load_dotenv


def get_secret():
    env = os.getenv("ENVIRONMENT", "local").lower()

# Load API key from .env for local development
load_dotenv()
API_KEY = os.getenv('API_KEY')


def get_dynamic_url(ticker):
    """
    Generate Polygon.io API URL for 1 year of daily bars for the given ticker.
    """
    today = datetime.date.today()
    one_year_ago = today - datetime.timedelta(days=365)
    return f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{one_year_ago}/{today}"


async def get_polygon_data(ticker, session):
    """
    Fetch 1 year of daily close data for the ticker from Polygon.io.
    Returns a DataFrame with a datetime index and 'close' column, or None on error.
    """
    url = get_dynamic_url(ticker)
    params = {"apiKey": API_KEY}
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with session.get(url, params=params, timeout=timeout) as response:
            if response.status != 200:
                print(f"Error fetching {ticker}: Status {response.status}")
                return None
            data = await response.json()
            if data.get('results'):
                df = pd.DataFrame(data['results'])
                df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
                df.set_index('timestamp', inplace=True)
                df['close'] = df['c']
                return df[['close']]
            else:
                return None
    except asyncio.TimeoutError:
        print(f"Timeout fetching {ticker}")
        return None
    except Exception as e:
        print(f"Exception fetching {ticker}: {e}")
        return None


def calculate_rsi(data, window=14):
    """
    Calculate the Relative Strength Index (RSI) for a pandas Series of prices.
    """
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


async def check_trend_template(ticker):
    """
    Check if a ticker meets all Mark Minervini Stage 2 technical criteria.
    Returns True if all conditions are met, False otherwise.
    """
    async with aiohttp.ClientSession() as session:
        hist = await get_polygon_data(ticker, session)
        if hist is None or hist.empty:
            return False
        hist['50_MA'] = hist['close'].rolling(window=50).mean()
        hist['150_MA'] = hist['close'].rolling(window=150).mean()
        hist['200_MA'] = hist['close'].rolling(window=200).mean()
        hist['RSI'] = calculate_rsi(hist['close'])
        latest = hist.iloc[-1]
        if any(pd.isna([latest['50_MA'], latest['150_MA'], latest['200_MA'], latest['RSI']])):
            return False
        if not (latest['close'] > latest['150_MA'] and latest['close'] > latest['200_MA']):
            return False
        if not (latest['150_MA'] > latest['200_MA']):
            return False
        if not (hist['200_MA'].iloc[-1] > hist['200_MA'].iloc[-5]):
            return False
        if not (latest['50_MA'] > latest['150_MA'] and latest['50_MA'] > latest['200_MA']):
            return False
        if not (latest['close'] > latest['50_MA']):
            return False
        min_52_week = hist['close'].min()
        if not (latest['close'] >= 1.3 * min_52_week):
            return False
        max_52_week = hist['close'].max()
        if not (latest['close'] >= 0.75 * max_52_week):
            return False
        if latest['RSI'] < 70:
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
        qualified = []
        now = datetime.datetime.now()
        today = now.date()
        time_now = now.time().replace(microsecond=0)
        session = SessionLocal()
        for ticker in tickers:
            print(f"Screening {ticker}...")
            if await check_trend_template(ticker):
                qualified.append(ticker)
                # Insert into DB
                entry = Stage2Data(
                    symbol=ticker,
                    data_date=today,
                    data_time=time_now,
                    data_json=json.dumps({"symbol": ticker, "date": str(today)})
                )
                session.add(entry)
        session.commit()
        session.close()
        print("Qualified Stage 2 stocks:", qualified)
    asyncio.run(main())
