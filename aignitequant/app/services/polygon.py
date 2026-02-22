"""
Polygon.io Data Service

Provides standardized access to Polygon.io market data API for all strategies.
Now uses the shared market_data DB table as primary source, falling back to
the Polygon API only when the DB has insufficient data.

Architecture:
    1. DB check (market_data table) — instant, populated by fetch_market_data task
    2. API fallback (Polygon.io) — only when DB is empty for a ticker

API: Polygon.io Aggregates (Bars) API
Requires: Polygon.io API key ($89+/month plan)
"""

import datetime
import aiohttp
import pandas as pd
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()
API_KEY = os.getenv("API_KEY")
print(f"✅ Polygon API key loaded: {API_KEY is not None}")

# Minimum number of rows expected for a valid dataset (most strategies need ~200)
_MIN_DB_ROWS = 50


def get_dynamic_url(ticker: str) -> str:
    """
    Generate Polygon.io API URL for 2 years of daily price data.
    
    Args:
        ticker (str): Stock ticker symbol
        
    Returns:
        str: Complete API URL for daily aggregates
    """
    today = datetime.date.today()
    two_years_ago = today - datetime.timedelta(days=730)
    return f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{two_years_ago}/{today}"


async def get_polygon_data(ticker: str, session: aiohttp.ClientSession):
    """
    Fetch historical OHLCV data — DB-first with Polygon.io API fallback.
    
    1. Checks the shared market_data DB table (populated by the centralized fetch task).
    2. Falls back to the Polygon.io API only if the DB has no/insufficient data.
    
    This means strategies continue to call this function exactly as before,
    but execution is near-instant when the fetch job has populated the DB.
    
    Args:
        ticker (str): Stock ticker symbol
        session (aiohttp.ClientSession): HTTP session (used only for API fallback)
        
    Returns:
        pd.DataFrame: DataFrame with columns [open, high, low, close, volume]
                     and datetime index, or None on error
    """
    # --- 1. Try DB first (fast path) ---
    try:
        from aignitequant.app.services.market_data import get_dataframe_from_db
        df = get_dataframe_from_db(ticker)
        if df is not None and len(df) >= _MIN_DB_ROWS:
            return df
    except Exception:
        pass  # DB not available — fall through to API
    
    # --- 2. Fallback to Polygon.io API (slow path) ---
    return await _fetch_from_polygon_api(ticker, session)


async def _fetch_from_polygon_api(ticker: str, session: aiohttp.ClientSession):
    """
    Direct Polygon.io API call — used as fallback when DB has insufficient data.
    This is the original get_polygon_data implementation.
    """
    url = get_dynamic_url(ticker)
    params = {"apiKey": API_KEY}
    
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with session.get(url, params=params, timeout=timeout) as response:
            if response.status != 200:
                error_text = await response.text()
                print(f"❌ Polygon API error for {ticker}: HTTP {response.status}, Response: {error_text}")
                return None
                
            data = await response.json()
            
            if data.get('results'):
                df = pd.DataFrame(data['results'])
                df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Standardize column names
                df['open'] = df['o']
                df['high'] = df['h']
                df['low'] = df['l']
                df['close'] = df['c']
                df['volume'] = df['v']
                
                print(f"✅ {ticker} data fetched, rows: {len(df)}")
                return df[['open', 'high', 'low', 'close', 'volume']]
            else:
                print(f"⚠️ No results returned for {ticker}")
                return None
                
    except Exception as e:
        print(f"❌ Error fetching {ticker} from Polygon: {e}")
        return None


async def fetch_polygon_close_async(tickers, days=2, end_date=None, batch_size=5, delay=1.0):
    """
    Fetch last N closing prices for multiple tickers efficiently.
    
    Now checks the shared market_data DB first, falling back to API only for
    tickers not found in the DB.
    
    Used by Follow The Money strategy for momentum calculations.
    
    Args:
        tickers (list): List of ticker symbols
        days (int): Number of recent closing prices to fetch (default: 2)
        end_date (str|date): Optional end date (YYYY-MM-DD) for historical data
        batch_size (int): Number of concurrent requests per batch (default: 5)
        delay (float): Seconds to wait between batches (default: 1.0)
        
    Returns:
        dict: Mapping of {ticker: [close1, close2, ...]} with most recent last
              Returns NaN values if insufficient data
              
    Example:
        closes = await fetch_polygon_close_async(['AAPL', 'MSFT'], days=2)
        # {'AAPL': [150.0, 152.5], 'MSFT': [350.0, 355.0]}
    """
    results = {}
    api_needed = []
    
    # --- 1. Try DB first for all tickers (fast batch read) ---
    try:
        from aignitequant.app.services.market_data import get_multiple_dataframes_from_db
        db_data = get_multiple_dataframes_from_db(list(tickers), days=730)
        
        for ticker in tickers:
            df = db_data.get(ticker)
            if df is not None and not df.empty:
                # Filter to end_date if specified
                if end_date:
                    end_dt = pd.to_datetime(end_date)
                    df = df[df.index <= end_dt]
                
                closes = df['close'].tail(days).tolist()
                if len(closes) < days:
                    closes = [float('nan')] * (days - len(closes)) + closes
                results[ticker] = closes
            else:
                api_needed.append(ticker)
    except Exception:
        api_needed = list(tickers)
    
    # --- 2. Fallback to API for missing tickers ---
    if not api_needed:
        return results

    async def fetch_last_n_closes(ticker):
        """Helper to fetch closes for a single ticker via API."""
        async with aiohttp.ClientSession() as session:
            df = await _fetch_from_polygon_api(ticker, session)
            
            if df is None or df.empty:
                return ticker, [float('nan')] * days
            
            # Filter to end_date if specified
            if end_date:
                if isinstance(end_date, str):
                    end_dt = pd.to_datetime(end_date)
                else:
                    end_dt = pd.to_datetime(end_date)
                df = df[df.index <= end_dt]
            
            # Get last N closes
            closes = df['close'].tail(days).tolist()
            
            # Pad with NaN if insufficient data
            if len(closes) < days:
                closes = [float('nan')] * (days - len(closes)) + closes
            
            return ticker, closes
    
    # Batch process API calls for rate limiting (only for tickers not in DB)
    for i in range(0, len(api_needed), batch_size):
        batch = api_needed[i:i+batch_size]
        tasks = [fetch_last_n_closes(t) for t in batch]
        batch_results = await asyncio.gather(*tasks)
        
        for ticker, closes in batch_results:
            results[ticker] = closes
        
        await asyncio.sleep(delay)  # Rate limiting delay
    
    return results
