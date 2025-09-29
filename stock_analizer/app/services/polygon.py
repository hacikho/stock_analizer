# Utility: fetch last N closes for a list of tickers, async and batched
async def fetch_polygon_close_async(tickers, days=2, end_date=None, batch_size=5, delay=1.0):
    """
    Fetch last N closes for each ticker. Returns {ticker: [close1, close2, ...]}
    If end_date is None, fetch up to latest; else up to end_date (YYYY-MM-DD or datetime.date).
    """
    import pandas as pd
    import datetime
    import aiohttp
    results = {}
    async def fetch_last_n_closes(ticker):
        async with aiohttp.ClientSession() as session:
            df = await get_polygon_data(ticker, session)
            if df is None or df.empty:
                return ticker, [float('nan')] * days
            if end_date:
                if isinstance(end_date, str):
                    end_dt = pd.to_datetime(end_date)
                else:
                    end_dt = pd.to_datetime(end_date)
                df = df[df.index <= end_dt]
            closes = df['close'].tail(days).tolist()
            # Pad if not enough data
            if len(closes) < days:
                closes = [float('nan')] * (days - len(closes)) + closes
            return ticker, closes
    # Use batch fetching for efficiency
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        tasks = [fetch_last_n_closes(t) for t in batch]
        batch_results = await asyncio.gather(*tasks)
        for ticker, closes in batch_results:
            results[ticker] = closes
        await asyncio.sleep(delay)
    return results
# app/services/polygon.py

import datetime
import aiohttp
import pandas as pd
import os
from dotenv import load_dotenv
import asyncio
import time

load_dotenv()
API_KEY = os.getenv("API_KEY")
print(f"✅ Polygon API key loaded: {API_KEY is not None}")

def get_dynamic_url(ticker: str) -> str:
    today = datetime.date.today()
    two_years_ago = today - datetime.timedelta(days=730)
    return f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{two_years_ago}/{today}"



# Simple in-memory cache: {ticker: (data, timestamp)}
polygon_cache = {}

CACHE_TTL = 60 * 10  # 10 minutes

async def fetch_polygon_data_batch(tickers, fetch_func, batch_size=5, delay=1.0):
    results = {}
    now = time.time()
    uncached = []

    # Check cache first
    for ticker in tickers:
        if ticker in polygon_cache and now - polygon_cache[ticker][1] < CACHE_TTL:
            results[ticker] = polygon_cache[ticker][0]
        else:
            uncached.append(ticker)

    # Batch fetch uncached tickers
    for i in range(0, len(uncached), batch_size):
        batch = uncached[i:i+batch_size]
        tasks = [fetch_func(t) for t in batch]
        batch_results = await asyncio.gather(*tasks)
        for ticker, data in zip(batch, batch_results):
            polygon_cache[ticker] = (data, time.time())
            results[ticker] = data
        await asyncio.sleep(delay)  # Wait between batches

    return results

async def get_polygon_data(ticker: str, session: aiohttp.ClientSession):
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
                df['close'] = df['c']
                # ADD THIS LINE:
                df['volume'] = df['v']
                print(f"✅ {ticker} data fetched, rows: {len(df)}")
                return df[['close', 'volume']]
            else:
                print(f"⚠️ No results returned for {ticker}")
                return None
    except Exception as e:
        print(f"❌ Error fetching {ticker} from Polygon: {e}")
        return None
