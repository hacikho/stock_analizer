# app/services/sp500.py

import pandas as pd
import aiohttp
import asyncio
import os
from cachetools import TTLCache
from dotenv import load_dotenv

load_dotenv()

sp500_cache = TTLCache(maxsize=2, ttl=3600)  # Increased cache size



async def get_sp500_from_wikipedia():
    """
    Get S&P 500 tickers from Wikipedia (the authoritative source for index constituents).
    """
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    import urllib.request
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    req = urllib.request.Request(url, headers=headers)
    
    try:
        # Read all tables from the page
        dfs = pd.read_html(urllib.request.urlopen(req), header=0)
        
        if not dfs:
            raise Exception("No tables found on Wikipedia page")
        
        print(f"📊 Found {len(dfs)} tables on Wikipedia page")
        
        # Try each table to find the S&P 500 constituent list
        for i, df in enumerate(dfs):
            print(f"� Table {i+1}: {df.shape[0]} rows, {df.shape[1]} columns")
            print(f"📋 Table {i+1} columns: {df.columns.tolist()[:3]}...")  # Show first 3 columns
            
            # Skip tables that are clearly not the constituent list
            if df.shape[0] < 100:  # S&P 500 should have ~500 rows
                print(f"⏭️ Skipping table {i+1}: too few rows ({df.shape[0]})")
                continue
                
            # Look for stock symbol patterns in the data
            symbol_col = None
            sector_col = None
            
            # More aggressive column detection
            for col in df.columns:
                col_str = str(col).lower().strip()
                # Check if this column contains stock-like symbols
                if not df[col].empty:
                    sample_values = df[col].dropna().astype(str).head(10).tolist()
                    # Look for ticker-like patterns (1-5 uppercase letters)
                    ticker_like = sum(1 for val in sample_values 
                                    if len(val) <= 5 and val.replace('-', '').replace('.', '').isalpha() 
                                    and val.isupper())
                    
                    if ticker_like >= 5:  # At least 5 ticker-like values
                        symbol_col = col
                        print(f"✅ Found symbol column: '{col}' (table {i+1})")
                        break
            
            # Look for sector column in this table
            for col in df.columns:
                col_str = str(col).lower().strip()
                if 'sector' in col_str or 'gics' in col_str:
                    sector_col = col
                    print(f"✅ Found sector column: '{col}' (table {i+1})")
                    break
            
            # If we found a symbol column, use this table
            if symbol_col is not None:
                print(f"🎯 Using table {i+1} for S&P 500 data")
                
                # Create sector column if not found
                if sector_col is None:
                    print("⚠️ No sector column found, creating placeholder")
                    df['GICS Sector'] = 'Technology'  # Default sector
                    sector_col = 'GICS Sector'
                
                try:
                    # Extract and clean the data
                    df_clean = df[[symbol_col, sector_col]].copy()
                    df_clean.columns = ['Symbol', 'GICS Sector']
                    
                    # Clean symbol data
                    df_clean['Symbol'] = df_clean['Symbol'].astype(str)
                    df_clean['Symbol'] = df_clean['Symbol'].str.replace('.', '-', regex=False)
                    df_clean['Symbol'] = df_clean['Symbol'].str.strip()
                    df_clean['Symbol'] = df_clean['Symbol'].str.upper()
                    
                    # Remove invalid entries
                    df_clean = df_clean.dropna(subset=['Symbol'])
                    df_clean = df_clean[df_clean['Symbol'] != '']
                    df_clean = df_clean[df_clean['Symbol'] != 'NAN']
                    df_clean = df_clean[df_clean['Symbol'].str.len() <= 6]  # Reasonable ticker length
                    df_clean = df_clean[df_clean['Symbol'].str.match(r'^[A-Z-]+$')]  # Only letters and hyphens
                    
                    if len(df_clean) >= 400:  # Should have most of S&P 500
                        print(f"✅ Wikipedia: Successfully parsed {len(df_clean)} S&P 500 tickers")
                        print(f"📋 Sample tickers: {df_clean['Symbol'].head().tolist()}")
                        return df_clean
                    else:
                        print(f"⚠️ Table {i+1} only has {len(df_clean)} valid tickers, continuing search...")
                        
                except Exception as e:
                    print(f"❌ Error processing table {i+1}: {e}")
                    continue
        
        # If no good table found, create a minimal fallback with major stocks
        print("⚠️ No suitable S&P 500 table found, using major stock fallback")
        major_stocks = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META', 'BRK.B',
            'UNH', 'XOM', 'JNJ', 'JPM', 'V', 'PG', 'MA', 'HD', 'CVX', 'ABBV',
            'LLY', 'BAC', 'AVGO', 'KO', 'PEP', 'TMO', 'COST', 'MRK', 'WMT',
            'ACN', 'ABT', 'NFLX', 'ADBE', 'CRM', 'VZ', 'DHR', 'TXN', 'NEE',
            'CMCSA', 'RTX', 'NKE', 'QCOM', 'PM', 'UPS', 'T', 'SPGI', 'HON'
        ]
        
        fallback_df = pd.DataFrame({
            'Symbol': major_stocks,
            'GICS Sector': 'Technology'  # Default sector
        })
        
        print(f"⚠️ Using fallback with {len(fallback_df)} major stocks")
        return fallback_df
        
    except Exception as e:
        raise Exception(f"Wikipedia scraping failed: {e}")


async def get_sp500_tickers(with_sector=False):
    """
    Get S&P 500 tickers from Wikipedia (primary source for index constituents).
    Polygon.io doesn't track S&P 500 membership, so Wikipedia is more reliable.
    """
    cache_key = "with_sector" if with_sector else "tickers_only"
    
    if cache_key in sp500_cache:
        print(f"✅ Using cached S&P 500 data")
        return sp500_cache[cache_key]
    
    df = None
    
    # Wikipedia is the primary source for S&P 500 constituents
    try:
        print("🔍 Fetching S&P 500 from Wikipedia...")
        df = await get_sp500_from_wikipedia()
        
        if df is None or df.empty:
            raise Exception("Wikipedia returned empty data")
            
    except Exception as e:
        print(f"❌ Wikipedia failed: {e}")
        raise Exception(f"Failed to fetch S&P 500 data from Wikipedia: {e}")
    
    # Cache the result
    if not with_sector:
        tickers = df['Symbol'].tolist()
        sp500_cache[cache_key] = tickers
        print(f"✅ Cached {len(tickers)} S&P 500 tickers")
        return tickers
    else:
        sp500_cache[cache_key] = df
        print(f"✅ Cached {len(df)} S&P 500 companies with sectors")
        return df

async def get_sector_map():
    """Returns a dict mapping sector name to list of tickers."""
    df = await get_sp500_tickers(with_sector=True)
    sector_map = df.groupby('GICS Sector')['Symbol'].apply(list).to_dict()
    return sector_map

def clear_sp500_cache():
    sp500_cache.clear()
