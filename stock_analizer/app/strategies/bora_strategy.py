import json
import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from app.db import SessionLocal, BoraData
from app.services.sp500 import get_sp500_tickers


"""
Bora Strategy: Enhanced Trend-Following Stock Screener with Volume Confirmation
------------------------------------------------------------------------------
This module implements a trend-following stock screening strategy based on the following rules:

TREND FILTERS:
1. The stock's price must be above its 200-day simple moving average (SMA_200).
2. The 21-day exponential moving average (EMA_21) must be above the 50-day EMA (EMA_50).
3. The EMA_21 must be trending up, as measured by slope, percent change, or strict monotonicity over a lookback window.

VOLUME & CONVICTION FILTERS:
4. Recent volume must be above the 20-day average volume (conviction behind moves).
5. On-Balance Volume (OBV) must be trending up (smart money accumulation).
6. Volume surge confirmation on recent green days (institutional interest).
7. Accumulation/Distribution Line must be positive (buying pressure > selling pressure).

The strategy can be run as a standalone script (for scheduled jobs) or imported as a module.
Results are stored in the BoraData table in the database for later retrieval via API.
"""

import pandas as pd
import numpy as np
import aiohttp
import asyncio
from app.services.polygon import get_polygon_data

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add technical indicators including moving averages and volume indicators.
    Args:
        df: DataFrame with 'close' and 'volume' columns.
    Returns:
        DataFrame with new indicator columns.
    """
    df = df.copy()
    
    # Price-based indicators
    df["SMA_200"] = df["close"].rolling(window=200).mean()
    df["EMA_21"]  = df["close"].ewm(span=21, adjust=False).mean()
    df["EMA_50"]  = df["close"].ewm(span=50, adjust=False).mean()
    
    # Volume-based indicators (only if volume data exists)
    if "volume" in df.columns and not df["volume"].isna().all():
        df["Volume_SMA_20"] = df["volume"].rolling(window=20).mean()
        
        # On-Balance Volume (OBV)
        df["Price_Change"] = df["close"].diff()
        df["OBV"] = (df["volume"] * np.where(df["Price_Change"] > 0, 1, 
                                            np.where(df["Price_Change"] < 0, -1, 0))).cumsum()
        
        # Accumulation/Distribution Line (only if we have OHLC data)
        if all(col in df.columns for col in ["high", "low"]):
            # Avoid division by zero
            high_low_diff = df["high"] - df["low"]
            high_low_diff = high_low_diff.replace(0, np.nan)
            
            df["High_Low_Close"] = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / high_low_diff
            df["AD_Line"] = (df["High_Low_Close"] * df["volume"]).cumsum()
        else:
            # If no OHLC data, set AD_Line to NaN
            df["AD_Line"] = np.nan
    else:
        # If no volume data, set all volume indicators to NaN
        df["Volume_SMA_20"] = np.nan
        df["OBV"] = np.nan
        df["AD_Line"] = np.nan
    
    return df

def volume_conviction_ok(df: pd.DataFrame, lookback=10) -> bool:
    """
    Check if volume indicators show conviction behind price movements.
    Args:
        df: DataFrame with volume indicators.
        lookback: Number of days to check for recent activity.
    Returns:
        True if volume conviction criteria are met, False otherwise.
    """
    try:
        # Check if we have volume data
        if "volume" not in df.columns or df["volume"].isna().all():
            return False
            
        # Check if recent volume is above average
        recent_avg_volume = df["volume"].iloc[-5:].mean()
        volume_avg_20 = df["Volume_SMA_20"].iloc[-1]
        
        if pd.isna(volume_avg_20) or pd.isna(recent_avg_volume) or recent_avg_volume <= volume_avg_20:
            return False
        
        # Check if OBV is trending up (only if OBV data exists)
        if "OBV" in df.columns and not df["OBV"].isna().all():
            obv_recent = df["OBV"].dropna().iloc[-lookback:]
            if len(obv_recent) >= lookback:
                obv_slope = np.polyfit(range(lookback), obv_recent.values, 1)[0]
                if obv_slope <= 0:
                    return False
        
        # Check for volume surge on green days
        recent_data = df.iloc[-lookback:]
        if len(recent_data) > 1:
            green_days = recent_data[recent_data["close"] > recent_data["close"].shift(1)]
            
            if len(green_days) > 0:
                green_volume_avg = green_days["volume"].mean()
                if pd.isna(green_volume_avg) or green_volume_avg <= volume_avg_20:
                    return False
        
        # Check A/D Line trend (only if available)
        if "AD_Line" in df.columns and not df["AD_Line"].isna().all():
            ad_recent = df["AD_Line"].dropna().iloc[-lookback:]
            if len(ad_recent) >= lookback:
                ad_slope = np.polyfit(range(len(ad_recent)), ad_recent.values, 1)[0]
                if ad_slope <= 0:
                    return False
        
        return True
        
    except Exception as e:
        # If any error occurs, skip volume checks and return False
        print(f"   ⚠️  Volume check failed for symbol: {str(e)}")
        return False

def ema21_trend_ok(df: pd.DataFrame, lookback=10, method="slope", slope_thresh=0.0, pct_thresh=1.0) -> bool:
    """
    Check if the EMA_21 is trending up over the lookback window.
    Args:
        df: DataFrame with 'EMA_21' column.
        lookback: Number of days to check.
        method: 'slope', 'pct', or 'strict'.
        slope_thresh: Minimum slope (for 'slope' method).
        pct_thresh: Minimum percent increase (for 'pct' method).
    Returns:
        True if trend criteria are met, False otherwise.
    """
    recent = df["EMA_21"].dropna().iloc[-lookback:]
    if len(recent) < lookback:
        return False

    if method == "slope":
        x = np.arange(lookback)
        y = recent.values
        slope, _ = np.polyfit(x, y, 1)
        return slope > slope_thresh

    elif method == "pct":
        pct_change = (recent.iloc[-1] / recent.iloc[0] - 1) * 100
        return pct_change > pct_thresh

    else:  # strict
        return all(x < y for x, y in zip(recent, recent[1:]))

async def scan_single_symbol(sym, session, method, slope_thresh, pct_thresh, lookback):
    """
    Screen a single symbol for Bora strategy criteria including volume confirmation.
    Args:
        sym: Stock symbol.
        session: aiohttp session.
        method, slope_thresh, pct_thresh, lookback: Trend parameters.
    Returns:
        Symbol if it passes all filters, else None.
    """
    df = await get_polygon_data(sym, session)
    if df is None or len(df) < 200:
        return None

    df = compute_indicators(df)
    try:
        price_last = df["close"].iloc[-1]
        sma200_last = df["SMA_200"].iloc[-1]
        ema21_last = df["EMA_21"].iloc[-1]
        ema50_last = df["EMA_50"].iloc[-1]
    except Exception:
        return None

    # Original trend filters (mandatory)
    if price_last <= sma200_last:
        return None
    if ema21_last <= ema50_last:
        return None
    if not ema21_trend_ok(df, lookback=lookback, method=method, slope_thresh=slope_thresh, pct_thresh=pct_thresh):
        return None
    
    # Volume & Conviction Filters (optional - only if volume data available)
    has_volume_data = "volume" in df.columns and not df["volume"].isna().all()
    if has_volume_data:
        if not volume_conviction_ok(df, lookback=lookback):
            return None
    # If no volume data, we still proceed with trend-only analysis

    return sym

async def scan_symbols(symbols, ema21_method="slope", slope_thresh=0.0, pct_thresh=1.0, lookback=10):
    """
    Run Bora strategy screen on a list of symbols.
    Args:
        symbols: List of stock symbols.
        ema21_method, slope_thresh, pct_thresh, lookback: Trend parameters.
    Returns:
        List of symbols that pass the Bora strategy.
    """
    picks = []
    async with aiohttp.ClientSession() as session:
        tasks = [
            scan_single_symbol(sym, session, ema21_method, slope_thresh, pct_thresh, lookback)
            for sym in symbols
        ]
        results = await asyncio.gather(*tasks)
        picks = [r for r in results if r]
    return picks
async def run_and_store_bora():
    """
    Run Bora strategy on S&P 500 tickers and store results in the database.
    """
    picks = []
    async with aiohttp.ClientSession() as session:
        tasks = [
            scan_single_symbol(sym, session, ema21_method, slope_thresh, pct_thresh, lookback)
            for sym in symbols
        ]
        results = await asyncio.gather(*tasks)
        picks = [r for r in results if r]
    return picks


async def run_and_store_bora():
    session = SessionLocal()
    try:
        print("🚀 Starting Bora Strategy Scan...")
        print("📈 Fetching S&P 500 tickers...")
        tickers = await get_sp500_tickers()
        print(f"📊 Found {len(tickers)} S&P 500 tickers to analyze")
        
        # Set default parameters for scan_symbols
        ema21_method = "slope"
        slope_thresh = 0.0
        pct_thresh = 1.0
        lookback = 10
        
        print(f"⚙️  Scan Parameters:")
        print(f"   • EMA21 Method: {ema21_method}")
        print(f"   • Slope Threshold: {slope_thresh}")
        print(f"   • Percent Threshold: {pct_thresh}%")
        print(f"   • Lookback Period: {lookback} days")
        print(f"   • Volume Filters: Above avg volume, OBV trend, A/D Line")
        print("🔍 Running enhanced screening analysis with volume confirmation...")
        
        picks = await scan_symbols(tickers, ema21_method=ema21_method, slope_thresh=slope_thresh, pct_thresh=pct_thresh, lookback=lookback)
        
        print(f"\n✅ Screening Complete! Found {len(picks)} qualifying stocks:")
        if picks:
            print("🏆 BORA Strategy Winners:")
            for i, sym in enumerate(picks, 1):
                print(f"   {i:2d}. {sym}")
        else:
            print("   No stocks met the criteria today")
        
        now = datetime.datetime.now()
        today = now.date()
        time_now = now.time().replace(microsecond=0)
        
        print(f"\n💾 Saving results to database...")
        print(f"📅 Date: {today}")
        print(f"🕒 Time: {time_now}")
        
        for i, sym in enumerate(picks, 1):
            entry = BoraData(
                symbol=sym,
                data_date=today,
                data_time=time_now,
                data_json=json.dumps({"Ticker": sym}),
            )
            session.add(entry)
            print(f"   💾 Added {sym} to database ({i}/{len(picks)})")
        
        session.commit()
        print(f"\n🎉 SUCCESS: Inserted {len(picks)} BORA results into DB for {today} {time_now}")
        
    except Exception as e:
        print(f"❌ Error in run_and_store_bora: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    """
    Entry point for running the Bora strategy as a script.
    Fetches S&P 500 tickers, runs the screen, and saves results to the database.
    """
    import asyncio
    asyncio.run(run_and_store_bora())