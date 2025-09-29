# app/services/sp500.py

import pandas as pd
from cachetools import TTLCache

sp500_cache = TTLCache(maxsize=1, ttl=3600)


async def get_sp500_tickers(with_sector=False):
    if not with_sector and "tickers" in sp500_cache:
        return sp500_cache["tickers"]
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    import urllib.request
    headers = {'User-Agent': 'Mozilla/5.0'}
    req = urllib.request.Request(url, headers=headers)
    dfs = pd.read_html(urllib.request.urlopen(req), header=0)
    df = dfs[0][['Symbol', 'GICS Sector']].copy()
    df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)  # For Yahoo/Polygon compatibility
    if not with_sector:
        tickers = df['Symbol'].tolist()
        sp500_cache["tickers"] = tickers
        return tickers
    return df

async def get_sector_map():
    """Returns a dict mapping sector name to list of tickers."""
    df = await get_sp500_tickers(with_sector=True)
    sector_map = df.groupby('GICS Sector')['Symbol'].apply(list).to_dict()
    return sector_map

def clear_sp500_cache():
    sp500_cache.clear()
