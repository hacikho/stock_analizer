import aiohttp
import asyncio
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

# Polygon.io endpoint for options trades summary (aggregate call/put volume)
def get_options_agg_url(ticker, date):
    return f"https://api.polygon.io/v3/reference/options/contracts?underlying_ticker={ticker}&as_of={date}&apiKey={API_KEY}"

async def fetch_options_activity(ticker, date, session):
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
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_options_activity(ticker, date, session) for ticker in tickers]
        results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

if __name__ == "__main__":
    import datetime
    # Example usage for today
    tickers = ["XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLK", "XLRE", "XLU", "XLB"]
    today = datetime.date.today().isoformat()
    options_data = asyncio.run(get_sector_options_activity(tickers, today))
    for ticker, data in options_data.items():
        print(f"{ticker}: {data}")
