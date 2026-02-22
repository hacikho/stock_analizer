"""
CANSLIM Strategy: Growth Stock Screener
---------------------------------------
This module implements a CANSLIM-based stock screening strategy, which evaluates stocks on:
    - Current and annual earnings growth
    - Market trend
    - Institutional sponsorship
    - Price strength and volume
    - Proximity to 52-week highs
    - Relative strength vs. the market

The strategy can be run as a standalone script (for scheduled jobs) or imported as a module.
Results are stored in the CanSlimData table in the database for later retrieval via API.
"""
import json
import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from app.db import SessionLocal, CanSlimData
from app.services.sp500 import get_sp500_tickers

import asyncio
import pandas as pd
import numpy as np
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.services.polygon import get_polygon_data
from app.services.market_data import get_dataframe_from_db, get_multiple_dataframes_from_db
import aiohttp

import time


polygon_cache = {}
finance_cache = {}
CACHE_TTL = 60 * 10  # 10 minutes
def fetch_yfinance_income_stmt(ticker):
    """
    Fetch the income statement for a ticker using yfinance, with caching.
    Args:
        ticker: Stock symbol.
    Returns:
        Pandas DataFrame of income statement, or None on error.
    """
    now = time.time()
    if ticker in finance_cache and now - finance_cache[ticker][1] < CACHE_TTL:
        return finance_cache[ticker][0]
    try:
        ticker_obj = yf.Ticker(ticker)
        income = ticker_obj.income_stmt
        finance_cache[ticker] = (income, time.time())
        return income
    except Exception as e:
        print(f"YFinance fetch error for {ticker}: {e}")
        return None

def batch_fetch_yfinance_income_stmts(tickers, max_workers=10):
    """
    Fetch income statements for a batch of tickers in parallel.
    Args:
        tickers: List of stock symbols.
        max_workers: Number of threads.
    Returns:
        Dict of {ticker: income statement DataFrame or None}.
    """
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(fetch_yfinance_income_stmt, t): t for t in tickers}
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                results[ticker] = future.result()
            except Exception as e:
                print(f"Threaded fetch error for {ticker}: {e}")
                results[ticker] = None
    return results

async def fetch_polygon_data_batch(tickers, session, fetch_func, batch_size=5, delay=1.0):
    """
    Fetch Polygon data for a batch of tickers asynchronously, with caching.
    Args:
        tickers: List of stock symbols.
        session: aiohttp session.
        fetch_func: Function to fetch data for a single ticker.
        batch_size: Number of tickers per batch.
        delay: Delay between batches (seconds).
    Returns:
        Dict of {ticker: DataFrame or None}.
    """
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
        tasks = [fetch_func(t, session) for t in batch]
        batch_results = await asyncio.gather(*tasks)
        for ticker, data in zip(batch, batch_results):
            polygon_cache[ticker] = (data, time.time())
            results[ticker] = data
        await asyncio.sleep(delay)  # Wait between batches

    return results

def quarterly_earnings_growth(income):
    """
    Calculate quarterly earnings growth percentage from income statement.
    Args:
        income: DataFrame with 'Net Income' row.
    Returns:
        Growth percentage or np.nan.
    """
    try:
        if income is None or "Net Income" not in income.index or income.shape[1] < 2:
            return np.nan
        net_income = income.loc["Net Income"]
        latest, prior = net_income.iloc[0], net_income.iloc[1]
        return (latest - prior) / abs(prior) * 100
    except Exception as e:
        print(f"Quarterly earnings error: {e}")
        return np.nan

def annual_earnings_growth(income):
    """
    Calculate annual earnings growth percentage from income statement.
    Args:
        income: DataFrame with 'Net Income' row.
    Returns:
        Growth percentage or np.nan.
    """
    try:
        if income is None or "Net Income" not in income.index or income.shape[1] < 4:
            return np.nan
        net_income = income.loc["Net Income"]
        latest_year = net_income.iloc[0:4].sum()
        if income.shape[1] >= 8:
            prior_year = net_income.iloc[4:8].sum()
            return (latest_year - prior_year) / abs(prior_year) * 100
        else:
            # Only one year available, return growth vs previous quarter
            prior = net_income.iloc[4] if len(net_income) > 4 else net_income.iloc[-1]
            return (latest_year - prior) / abs(prior) * 100 if prior != 0 else np.nan
    except Exception as e:
        print(f"Annual earnings error: {e}")
        return np.nan

def institutional_holders(ticker_obj):
    """
    Analyze institutional holders for a ticker, focusing on reputable institutions.
    Args:
        ticker_obj: yfinance Ticker object.
    Returns:
        Dict with total value, reputable count, and names.
    """
    try:
        holders = ticker_obj.institutional_holders
        print(f"{ticker_obj.ticker} institutional_holders: {holders}")
        reputable_names = [
            'Vanguard', 'Blackrock', 'State Street', 'Fidelity', 'T. Rowe', 'Invesco',
            'Morgan Stanley', 'JPMORGAN', 'Bank of America', 'Wellington', 'Geode',
            'Northern Trust', 'Goldman Sachs', 'UBS', 'Franklin', 'Charles Schwab',
            'Capital Group', 'Dodge & Cox', 'Massachusetts Financial', 'FMR', 'FIL LTD',
            'Price (T.Rowe)', 'Ameriprise', 'Alliancebernstein', 'Fundsmith', 'NORGES BANK',
            'Cohen & Steers', 'Parnassus', 'Susquehanna', 'Victory Capital', 'Apg Asset Management',
            'Hotchkis & Wiley', 'Polen Capital', 'Kayne Anderson', 'Leonard Green', 'Renaissance Technologies',
            'State Farm', 'Aristotle Capital', 'First Eagle', 'Boston Partners', 'Slate Path Capital',
            'Durable Capital', 'Artisan Partners', 'Brown Advisory', 'Barrow, Hanley', 'Harris Associates',
            'Primecap', 'Fundsmith', 'Impax Asset', 'Pictet Asset', 'Amundi', 'Royal Bank of Canada',
            'Bank Of New York Mellon', 'Fundsmith Investment', 'Fundsmith LLP', 'Fundsmith Investment Services',
        ]
        reputable_count = 0
        reputable_set = set()
        total_value = 0.0
        if holders is not None and 'Value' in holders.columns and 'Holder' in holders.columns:
            total_value = holders['Value'].sum()
            for name in holders['Holder']:
                for rep in reputable_names:
                    if rep.lower() in str(name).lower():
                        reputable_set.add(rep)
            reputable_count = len(reputable_set)
            print(f"{ticker_obj.ticker} total institutional value: {total_value}, reputable holders: {reputable_set}")
            return {"total_value": float(total_value), "reputable_count": reputable_count, "reputable_names": list(reputable_set)}
        elif holders is not None:
            # Fallback: just count unique holders
            return {"total_value": 0.0, "reputable_count": len(set(holders['Holder'])) if 'Holder' in holders.columns else len(holders), "reputable_names": list(set(holders['Holder'])) if 'Holder' in holders.columns else []}
        else:
            return {"total_value": 0.0, "reputable_count": 0, "reputable_names": []}
    except Exception as e:
        print(f"Institutional holders error: {e}")
        return {"total_value": 0.0, "reputable_count": 0, "reputable_names": []}

def is_near_52w_high(df, threshold=0.95):
    """
    Check if the last close is within a threshold of the 52-week high.
    Args:
        df: DataFrame with 'close' column.
        threshold: Fraction of 52w high (e.g., 0.95 = within 5%).
    Returns:
        True if near high, else False.
    """
    if "close" not in df.columns:
        print("⚠️ Close data missing")
        return False
    high_52 = df["close"].max()
    last_close = df["close"].iloc[-1]
    return last_close >= high_52 * threshold

def volume_spike(df, window_short=20, window_long=50, pct_increase=30):
    """
    Check for a volume spike in recent trading.
    Args:
        df: DataFrame with 'volume' column.
        window_short: Recent window size.
        window_long: Baseline window size.
        pct_increase: Minimum percent increase.
    Returns:
        True if spike detected, else False.
    """
    if "volume" not in df.columns:
        print("⚠️ Volume data missing")
        return False
    if len(df) < window_long:
        return False
    recent = df["volume"].tail(window_short).mean()
    prior = df["volume"].tail(window_long).head(window_long - window_short).mean()
    return (recent - prior) / prior * 100 >= pct_increase

def market_trend_ok_from_db():
    """
    Check if the S&P 500 is in an uptrend (MA50 > MA200).
    Reads SPY data from the shared market_data DB table (no API call).
    Returns:
        True if uptrend, else False.
    """
    df = get_dataframe_from_db("SPY")
    if df is None or len(df) < 200:
        print("Market trend check: insufficient data in DB")
        return False
    ma50 = df["close"].rolling(50).mean().iloc[-1]
    ma200 = df["close"].rolling(200).mean().iloc[-1]
    print(f"S&P 500 MA50: {ma50:.2f}, MA200: {ma200:.2f}")
    return ma50 > ma200


async def market_trend_ok(session):
    """
    Legacy async wrapper — checks DB first, falls back to API.
    """
    result = market_trend_ok_from_db()
    if result is not None:
        return result
    # Fallback to API if DB has no SPY data
    df = await get_polygon_data("SPY", session)
    if df is None or len(df) < 200:
        print("Market trend check: insufficient data")
        return False
    ma50 = df["close"].rolling(50).mean().iloc[-1]
    ma200 = df["close"].rolling(200).mean().iloc[-1]
    print(f"S&P 500 MA50: {ma50}, MA200: {ma200}")
    return ma50 > ma200

async def relative_strength(df_t, df_m, period_days=120):
    """
    Calculate relative strength of a ticker vs. the market.
    Args:
        df_t: Ticker DataFrame.
        df_m: Market DataFrame.
        period_days: Lookback period.
    Returns:
        Relative strength ratio or np.nan.
    """
    if df_t is None or df_m is None:
        return np.nan
    if len(df_t) < period_days or len(df_m) < period_days:
        return np.nan
    gain_t = df_t["close"].iloc[-1] / df_t["close"].iloc[-period_days] - 1
    gain_m = df_m["close"].iloc[-1] / df_m["close"].iloc[-period_days] - 1
    return gain_t / gain_m if gain_m != 0 else np.nan

def compute_ibd_rs_raw(df, min_days=252):
    """
    Compute the raw IBD-style weighted relative strength score for a single ticker.
    
    Uses IBD's multi-period weighting:
        - 40% weight: current quarter (~63 trading days)
        - 20% weight: prior quarter (63–126 days ago)
        - 20% weight: 2 quarters ago (126–189 days ago)
        - 20% weight: 3 quarters ago (189–252 days ago)
    
    Args:
        df: DataFrame with 'close' column for the ticker.
        min_days: Minimum trading days required (default 252 = ~1 year).
    Returns:
        Weighted performance score (float), or np.nan if insufficient data.
    """
    if df is None or len(df) < min_days:
        return np.nan
    
    closes = df["close"].values
    current = closes[-1]
    
    # Quarter boundaries (trading days)
    q1_start = closes[-63]   # ~3 months ago
    q2_start = closes[-126]  # ~6 months ago
    q3_start = closes[-189]  # ~9 months ago
    q4_start = closes[-252]  # ~12 months ago
    
    # Performance for each quarter
    perf_q1 = (current / q1_start - 1) * 100       # Current quarter
    perf_q2 = (q1_start / q2_start - 1) * 100      # Prior quarter
    perf_q3 = (q2_start / q3_start - 1) * 100      # 2 quarters ago
    perf_q4 = (q3_start / q4_start - 1) * 100      # 3 quarters ago
    
    # IBD weighting: 2x weight on most recent quarter
    weighted_score = (perf_q1 * 0.4) + (perf_q2 * 0.2) + (perf_q3 * 0.2) + (perf_q4 * 0.2)
    return weighted_score

def compute_ibd_rs_ratings(ticker_data_dict):
    """
    Compute IBD-style RS Ratings (1-99 percentile) for all tickers.
    
    This ranks each stock's weighted performance score against all others
    in the universe and converts to a 1-99 percentile scale, matching
    IBD's methodology.
    
    Args:
        ticker_data_dict: Dict of {ticker: DataFrame} for all tickers in the universe.
    Returns:
        Dict of {ticker: rs_rating (int 1-99)} for tickers with sufficient data.
    """
    # Step 1: Compute raw weighted score for each ticker
    raw_scores = {}
    for ticker, df in ticker_data_dict.items():
        score = compute_ibd_rs_raw(df)
        if not np.isnan(score):
            raw_scores[ticker] = score
    
    if not raw_scores:
        return {}
    
    # Step 2: Rank and convert to percentile (1-99)
    scores_series = pd.Series(raw_scores)
    # percent=True gives values 0-1, multiply by 100 and clamp to 1-99
    percentile_ranks = scores_series.rank(pct=True) * 100
    rs_ratings = percentile_ranks.clip(1, 99).round(0).astype(int).to_dict()
    
    print(f"📊 Computed IBD RS Ratings for {len(rs_ratings)} tickers (universe size: {len(raw_scores)})")
    
    return rs_ratings

async def screen_ticker_with_df(ticker, session, market_df, df):
    """
    Screen a single ticker for CANSLIM criteria (legacy version).
    Args:
        ticker: Stock symbol.
        session: aiohttp session.
        market_df: Market DataFrame.
        df: Ticker DataFrame.
    Returns:
        Dict of results if passed, else None.
    """
    try:
        print(f"{ticker} columns: {df.columns}")
        if df is None or len(df) < 200:
            return None

        ticker_obj = yf.Ticker(ticker)
        income = ticker_obj.income_stmt

        q = quarterly_earnings_growth(income)
        y = annual_earnings_growth(income)
        near_52w = is_near_52w_high(df)
        vol = volume_spike(df)
        rs = await relative_strength(df, market_df)
        holders = institutional_holders(ticker_obj)
        row = {
            "Ticker": str(ticker),
            "QtrEarningsGrowth%": float(q) if not pd.isna(q) else None,
            "YrEarningsGrowth%": float(y) if not pd.isna(y) else None,
            "Near52WHigh": bool(near_52w),
            "VolumeUp30%": bool(vol),
            "RelStrength": float(rs) if not pd.isna(rs) else None,
            "InstHolders": int(holders),
        }
        if (
            q >= 25 and
            y >= 25 and
            near_52w and
            vol and
            rs >= 1.0 and
            holders >= 3
        ):
            return row

        print(f"▶️ {ticker}: Qtr={q}, Yr={y}, RS={rs}, Near52W={near_52w}, VolSpike={vol}, Holders={holders}")
        return None

    except Exception as e:
        print(f"Error screening {ticker}: {e}")
        return None

async def canslim_screen(tickers):
    """
    Run CANSLIM screen on a list of tickers and return those that pass.
    Now reads all price data from the shared market_data DB table.
    Only yfinance calls go over the network (for fundamentals/earnings).
    
    Args:
        tickers: List of stock symbols.
    Returns:
        List of dicts with passing tickers and their metrics.
    """
    import time
    start_total = time.time()
    async with aiohttp.ClientSession() as session:
        print("🔍 Checking S&P 500 market trend...")
        if not market_trend_ok_from_db():
            print("⛔ Market not in uptrend. Blocking all CANSLIM picks.")
            return []

        # Batch read all ticker data from shared market_data DB (single query)
        t0 = time.time()
        all_symbols = list(set(tickers + ["SPY"]))
        all_data = get_multiple_dataframes_from_db(all_symbols)
        market_df = all_data.get("SPY")
        ticker_data = {t: all_data.get(t) for t in tickers if all_data.get(t) is not None}
        db_hits = len(ticker_data)
        print(f"📂 DB batch read: {db_hits}/{len(tickers)} tickers loaded in {time.time() - t0:.2f}s")

        # Batch fetch all yfinance financials (income statements) — still needs network
        t1 = time.time()
        yfinance_income = batch_fetch_yfinance_income_stmts(tickers)
        print(f"YFinance batch fetch took {time.time() - t1:.2f}s")

        # Compute IBD-style RS Ratings across the full universe
        t2 = time.time()
        rs_ratings = compute_ibd_rs_ratings(ticker_data)
        print(f"IBD RS Rating computation took {time.time() - t2:.2f}s")

        async def screen_ticker_with_df_fast(ticker, session, market_df, df, income):
            try:
                print(f"{ticker} columns: {df.columns}")
                if df is None or len(df) < 200:
                    return None

                ticker_obj = yf.Ticker(ticker)
                # Diagnostics for income statement
                if income is not None:
                    print(f"{ticker} income shape: {income.shape}, index: {income.index}")
                else:
                    print(f"{ticker} income is None")

                q = quarterly_earnings_growth(income)
                y = annual_earnings_growth(income)
                near_52w = is_near_52w_high(df, threshold=0.90)  # Lowered threshold to 90%
                vol = volume_spike(df, pct_increase=15)  # Lowered volume spike to 15%
                rs = await relative_strength(df, market_df)
                rs_rating = rs_ratings.get(ticker, None)
                holders_info = institutional_holders(ticker_obj)
                row = {
                    "Ticker": str(ticker),
                    "QtrEarningsGrowth%": float(q) if not pd.isna(q) else None,
                    "YrEarningsGrowth%": float(y) if not pd.isna(y) else None,
                    "Near52WHigh": bool(near_52w),
                    "VolumeUp15%": bool(vol),
                    "RelStrength": float(rs) if not pd.isna(rs) else None,
                    "RS_Rating": rs_rating,
                    "InstHoldersValue": float(holders_info["total_value"]),
                    "InstReputableCount": holders_info["reputable_count"],
                    "InstReputableNames": holders_info["reputable_names"],
                }
                # CANSLIM "I" filter: require at least $1B and at least 2 reputable institutions
                # RS Rating >= 80 matches IBD's recommendation for CANSLIM leaders
                if (
                    q >= 25 and
                    (pd.isna(y) or y >= 25) and
                    near_52w and
                    vol and
                    (rs_rating is not None and rs_rating >= 80) and
                    holders_info["total_value"] >= 1e9 and
                    holders_info["reputable_count"] >= 2
                ):
                    return row

                print(f"▶️ {ticker}: Qtr={q}, Yr={y}, RS={rs}, RS_Rating={rs_rating}, Near52W={near_52w}, VolSpike={vol}, InstValue={holders_info['total_value']}, InstReputable={holders_info['reputable_names']}")
                return None

            except Exception as e:
                print(f"Error screening {ticker}: {e}")
                return None

        # Launch all screening tasks in parallel
        tasks = [
            screen_ticker_with_df_fast(t, session, market_df, ticker_data.get(t), yfinance_income.get(t))
            for t in tickers
        ]
        results = await asyncio.gather(*tasks)
        print(f"Total CANSLIM screening took {time.time() - start_total:.2f}s")
        return [r for r in results if r]
    



async def run_and_store_canslim():
    """
    Run CANSLIM strategy on S&P 500 tickers and store results in the database.
    """
    session = SessionLocal()
    try:
        tickers = await get_sp500_tickers()
        results = await canslim_screen(tickers)
        now = datetime.datetime.now()
        today = now.date()
        time_now = now.time().replace(microsecond=0)
        for row in results:
            entry = CanSlimData(
                symbol=row.get("Ticker"),
                data_date=today,
                data_time=time_now,
                data_json=json.dumps(row),
            )
            session.add(entry)
        session.commit()
        print(f"Inserted {len(results)} CANSLIM results into DB for {today} {time_now}")
    except Exception as e:
        print(f"Error in run_and_store_canslim: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    """
    Entry point for running the CANSLIM strategy as a script.
    Fetches S&P 500 tickers, runs the screen, and saves results to the database.
    """
    import asyncio
    asyncio.run(run_and_store_canslim())