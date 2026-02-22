"""
Polygon.io Options Activity Service

Fetches options contract data for multiple tickers concurrently.
Used by Follow The Money strategy to analyze institutional options activity.

API Endpoint: Polygon.io Options Contracts Reference
Requires: Polygon.io API key ($89+/month plan)
"""

import aiohttp
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")


def get_options_agg_url(ticker, date):
    """
    Build Polygon.io options contracts API URL.
    
    Args:
        ticker (str): Stock ticker symbol
        date (str): Date in ISO format (YYYY-MM-DD)
        
    Returns:
        str: Complete API URL with authentication
    """
    return f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={ticker}&as_of={date}&apiKey={API_KEY}"


async def fetch_options_activity(ticker, date, session):
    """
    Fetch options activity for a single ticker asynchronously.
    
    Args:
        ticker (str): Stock ticker symbol
        date (str): Date in ISO format (YYYY-MM-DD)
        session (aiohttp.ClientSession): HTTP session for connection pooling
        
    Returns:
        dict: Options contract data from Polygon.io, or None on error
    """
    url = get_options_agg_url(ticker, date)
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Polygon API error for {ticker}: HTTP {response.status}")
                return None
            data = await response.json()
            return data
    except Exception as e:
        print(f"Error fetching options data for {ticker}: {e}")
        return None


async def get_sector_options_activity(tickers, date):
    """
    Fetch options activity for multiple tickers concurrently.
    
    Used by Follow The Money strategy to analyze institutional options positioning
    across sector ETFs (XLC, XLY, XLP, etc.).
    
    Args:
        tickers (list): List of ticker symbols
        date (str): Date in ISO format (YYYY-MM-DD)
        
    Returns:
        dict: Mapping of {ticker: options_data} for all tickers
    """
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_options_activity(ticker, date, session) for ticker in tickers]
        results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))


if __name__ == "__main__":
    """
    Standalone test script for debugging options data fetching.
    Usage: python -m aignitequant.app.services.polygon_options
    """
    import datetime
    
    # Test with sector ETFs
    tickers = ["XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLK", "XLRE", "XLU", "XLB"]
    today = datetime.date.today().isoformat()
    
    print(f"Fetching options data for {len(tickers)} tickers on {today}...")
    options_data = asyncio.run(get_sector_options_activity(tickers, today))
    
    for ticker, data in options_data.items():
        if data:
            print(f"{ticker}: ✓ {len(data.get('results', []))} contracts")
        else:
            print(f"{ticker}: ✗ No data")
