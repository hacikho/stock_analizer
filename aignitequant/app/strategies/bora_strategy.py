import json
import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from app.db import SessionLocal, BoraData, BoraPosition
from app.services.sp500 import get_sp500_tickers
from sqlalchemy import delete


"""
Bora Strategy: Risk-Managed Trend-Following Stock Screener with Entry Timing
----------------------------------------------------------------------------
This module implements a trend-following stock screening strategy based on the following rules:

TREND FILTERS:
1. The stock's price must be above its 200-day simple moving average (SMA_200).
2. The 21-day exponential moving average (EMA_21) must be above the 50-day EMA (EMA_50).
3. The EMA_21 must be trending up, as measured by slope, percent change, or strict monotonicity over a lookback window.

VOLATILITY & RISK MANAGEMENT FILTERS:
4. ATR-based filters: Avoid overly volatile stocks (ATR < X% of price).
5. Bollinger Band position: Price in upper half but not touching upper band.
6. Beta filter: Moderate beta (0.8-1.5) - responsive but not crazy volatile.
7. Maximum correlation with VIX: Low correlation with fear index.

VOLUME & CONVICTION FILTERS:
8. Recent volume must be above the 20-day average volume (conviction behind moves).
9. On-Balance Volume (OBV) must be trending up (smart money accumulation).
10. Volume surge confirmation on recent green days (institutional interest).
11. Accumulation/Distribution Line must be positive (buying pressure > selling pressure).

ENTRY TIMING ENHANCEMENTS:
12. Pullback opportunity: Recently pulled back to EMA_21 and bounced.
13. Consolidation breakout: Breaking out of a consolidation pattern.
14. Gap-up confirmation: Recent gap-ups followed by continuation.

The strategy can be run as a standalone script (for scheduled jobs) or imported as a module.
Results are stored in the BoraData table in the database for later retrieval via API.
"""

import pandas as pd
import numpy as np
import aiohttp
import asyncio
from app.services.polygon import get_polygon_data
from app.services.market_data import get_dataframe_from_db, get_multiple_dataframes_from_db

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add technical indicators including moving averages, volume, volatility, and timing indicators.
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
    
    # Entry timing indicators
    # Distance from EMA_21 (for pullback detection)
    df["Distance_EMA21"] = ((df["close"] - df["EMA_21"]) / df["EMA_21"]) * 100
    
    # Consolidation detection - range compression
    df["High_10"] = df["close"].rolling(window=10).max()
    df["Low_10"] = df["close"].rolling(window=10).min()
    df["Range_10"] = df["High_10"] - df["Low_10"]
    df["Range_Pct_10"] = (df["Range_10"] / df["Low_10"]) * 100
    
    # Gap detection (if we have OHLC data)
    if "open" in df.columns:
        df["Gap"] = ((df["open"] - df["close"].shift(1)) / df["close"].shift(1)) * 100
    else:
        # Simulate gap using close prices
        df["Gap"] = ((df["close"] - df["close"].shift(1)) / df["close"].shift(1)) * 100
    
    # Volatility indicators
    # ATR (Average True Range)
    if all(col in df.columns for col in ["high", "low"]):
        df["TR1"] = df["high"] - df["low"]
        df["TR2"] = abs(df["high"] - df["close"].shift(1))
        df["TR3"] = abs(df["low"] - df["close"].shift(1))
        df["TR"] = df[["TR1", "TR2", "TR3"]].max(axis=1)
        df["ATR"] = df["TR"].rolling(window=14).mean()
    else:
        df["ATR"] = np.nan
    
    # Bollinger Bands
    sma_20 = df["close"].rolling(window=20).mean()
    std_20 = df["close"].rolling(window=20).std()
    df["BB_Upper"] = sma_20 + (2 * std_20)
    df["BB_Lower"] = sma_20 - (2 * std_20)
    df["BB_Middle"] = sma_20
    
    # Bollinger Band position (0 = lower band, 1 = upper band)
    df["BB_Position"] = (df["close"] - df["BB_Lower"]) / (df["BB_Upper"] - df["BB_Lower"])
    
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

def volatility_risk_ok(df: pd.DataFrame, max_atr_pct=5.0, min_beta=0.8, max_beta=1.5, max_vix_corr=0.3) -> bool:
    """
    Check if volatility and risk management criteria are met.
    Args:
        df: DataFrame with volatility indicators.
        max_atr_pct: Maximum ATR as percentage of price.
        min_beta, max_beta: Beta range for moderate volatility.
        max_vix_corr: Maximum correlation with VIX (fear index).
    Returns:
        True if volatility/risk criteria are met, False otherwise.
    """
    try:
        current_price = df["close"].iloc[-1]
        
        # 1. ATR-based filter (avoid overly volatile stocks)
        if "ATR" in df.columns and not df["ATR"].isna().all():
            current_atr = df["ATR"].iloc[-1]
            if not pd.isna(current_atr):
                atr_pct = (current_atr / current_price) * 100
                if atr_pct > max_atr_pct:
                    return False
        
        # 2. Bollinger Band position (upper half but not touching upper band)
        if "BB_Position" in df.columns:
            bb_position = df["BB_Position"].iloc[-1]
            if not pd.isna(bb_position):
                # Should be in upper half (> 0.5) but not at the top (< 0.95)
                if bb_position <= 0.5 or bb_position >= 0.95:
                    return False
        
        # 3. Beta filter - simulate moderate volatility check
        # Using price volatility as proxy for beta (20-day rolling std)
        returns = df["close"].pct_change().dropna()
        if len(returns) >= 20:
            volatility_20d = returns.rolling(20).std().iloc[-1]
            # Typical market volatility is around 1-2% daily
            # Moderate stocks should have volatility between 1-3% (proxy for 0.8-1.5 beta)
            if not pd.isna(volatility_20d):
                volatility_pct = volatility_20d * 100
                if volatility_pct < 1.0 or volatility_pct > 3.5:
                    return False
        
        # 4. VIX correlation filter - simplified check
        # Since we don't have VIX data easily, we'll use a volatility stability check
        # Check that volatility hasn't spiked recently (which would indicate fear correlation)
        if len(returns) >= 10:
            recent_vol = returns.iloc[-5:].std()
            longer_vol = returns.iloc[-20:-5].std() if len(returns) >= 20 else returns.iloc[-10:].std()
            
            if not pd.isna(recent_vol) and not pd.isna(longer_vol) and longer_vol > 0:
                vol_spike_ratio = recent_vol / longer_vol
                # If recent volatility is more than 2x normal, likely fear-driven
                if vol_spike_ratio > 2.0:
                    return False
        
        return True
        
    except Exception as e:
        print(f"   ⚠️  Volatility check failed: {str(e)}")
        return False

def entry_timing_ok(df: pd.DataFrame, lookback=10) -> bool:
    """
    Check if entry timing indicators suggest a good entry opportunity.
    Args:
        df: DataFrame with timing indicators.
        lookback: Number of days to check for patterns.
    Returns:
        True if entry timing criteria are met, False otherwise.
    """
    try:
        current_price = df["close"].iloc[-1]
        
        # 1. Pullback opportunity: Recently pulled back to EMA_21 and bounced
        if "Distance_EMA21" in df.columns and "EMA_21" in df.columns:
            recent_distances = df["Distance_EMA21"].iloc[-lookback:]
            current_distance = df["Distance_EMA21"].iloc[-1]
            
            if not pd.isna(current_distance):
                # Check if we recently touched/got close to EMA_21 (within 2%) and bounced back
                min_distance_recent = recent_distances.min()
                if min_distance_recent <= 2.0 and current_distance > min_distance_recent:
                    # Found a pullback and bounce pattern
                    pass
                else:
                    # Also accept if currently close to EMA_21 but trending up
                    if current_distance > 5.0:  # Too far from EMA_21
                        return False
        
        # 2. Consolidation breakout: Breaking out of a consolidation pattern
        if "Range_Pct_10" in df.columns and "High_10" in df.columns:
            recent_ranges = df["Range_Pct_10"].iloc[-lookback:]
            current_high_10 = df["High_10"].iloc[-2]  # Previous day's 10-day high
            
            if not pd.isna(current_high_10) and len(recent_ranges) >= 5:
                # Check for recent consolidation (low volatility)
                avg_range = recent_ranges.mean()
                if not pd.isna(avg_range) and avg_range < 8.0:  # Relatively tight range
                    # Check if breaking out above recent high
                    if current_price > current_high_10:
                        # Breakout confirmed
                        pass
                    # Also accept if very close to breakout
                    elif current_price > (current_high_10 * 0.995):
                        pass
                    else:
                        return False
        
        # 3. Gap-up confirmation: Recent gap-ups followed by continuation
        if "Gap" in df.columns:
            recent_gaps = df["Gap"].iloc[-5:]  # Last 5 days
            recent_prices = df["close"].iloc[-5:]
            
            if len(recent_gaps) >= 3:
                # Look for recent gap up (> 1%) followed by continuation
                gap_up_days = recent_gaps[recent_gaps > 1.0]
                
                if len(gap_up_days) > 0:
                    # Find the most recent gap up
                    gap_day_idx = recent_gaps[recent_gaps > 1.0].index[-1]
                    gap_day_pos = list(recent_gaps.index).index(gap_day_idx)
                    
                    # Check if price continued higher after gap
                    if gap_day_pos < len(recent_prices) - 1:
                        price_after_gap = recent_prices.iloc[gap_day_pos + 1:]
                        if len(price_after_gap) > 0:
                            gap_price = recent_prices.iloc[gap_day_pos]
                            continuation = price_after_gap.iloc[-1] >= gap_price
                            if not continuation:
                                return False
        
        return True
        
    except Exception as e:
        print(f"   ⚠️  Entry timing check failed: {str(e)}")
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

def check_exit_signals(df: pd.DataFrame, symbol: str, entry_price: float, entry_date) -> tuple:
    """
    Check if any exit conditions are met for an active position.
    
    IMMEDIATE EXIT CONDITIONS:
    1. 6% stop loss hit
    2. EMA_21 crosses below EMA_50 (trend reversal)
    3. Volatility spike > 2x normal
    
    Returns:
        (should_exit: bool, exit_reason: str)
    """
    try:
        if len(df) < 50:
            return False, ""
        
        current_price = df['close'].iloc[-1]
        
        # PRIORITY 1: 6% Stop Loss (IMMEDIATE EXIT)
        loss_pct = ((current_price - entry_price) / entry_price) * 100
        if loss_pct <= -6.0:
            return True, f"STOP LOSS: {loss_pct:.1f}% loss"
        
        # PRIORITY 2: EMA_21 crosses below EMA_50 (trend reversal)
        if 'EMA_21' in df.columns and 'EMA_50' in df.columns:
            ema_21 = df['EMA_21'].iloc[-1]
            ema_50 = df['EMA_50'].iloc[-1]
            ema_21_prev = df['EMA_21'].iloc[-2]
            ema_50_prev = df['EMA_50'].iloc[-2]
            
            if ema_21_prev >= ema_50_prev and ema_21 < ema_50:
                return True, "TREND REVERSAL: EMA_21 crossed below EMA_50"
        
        # PRIORITY 3: Volatility spike > 2x normal
        returns = df['close'].pct_change().dropna()
        if len(returns) >= 20:
            recent_vol = returns.iloc[-5:].std()
            longer_vol = returns.iloc[-20:-5].std()
            
            if not pd.isna(recent_vol) and not pd.isna(longer_vol) and longer_vol > 0:
                vol_spike_ratio = recent_vol / longer_vol
                if vol_spike_ratio > 2.0:
                    return True, f"VOLATILITY SPIKE: {vol_spike_ratio:.1f}x normal"
        
        # SECONDARY: Price breaks 3% below EMA_21 (trend weakening)
        if 'EMA_21' in df.columns:
            ema_21 = df['EMA_21'].iloc[-1]
            if current_price < (ema_21 * 0.97):
                return True, "Broke 3% below EMA_21 (trend weakening)"
        
        # PROFIT TARGET: 20% gain
        profit_pct = ((current_price - entry_price) / entry_price) * 100
        if profit_pct >= 20.0:
            return True, f"PROFIT TARGET: +{profit_pct:.1f}% gain"
        
        return False, ""
        
    except Exception as e:
        print(f"   ⚠️  Exit check failed for {symbol}: {str(e)}")
        return False, ""


async def check_and_exit_positions():
    """
    Check all active Bora positions for exit signals.
    Remove positions from database when exit conditions are met.
    """
    db_session = SessionLocal()
    try:
        print("\n🔍 Checking active Bora positions for exit signals...")
        
        # Get all active positions
        positions = db_session.query(BoraPosition).all()
        
        if not positions:
            print("   No active positions to check")
            return
        
        print(f"   Found {len(positions)} active positions")
        
        # Batch read all position symbols from DB in one query
        position_symbols = [p.symbol for p in positions]
        all_position_data = get_multiple_dataframes_from_db(position_symbols)
        
        exits = []
        for position in positions:
            symbol = position.symbol
            entry_price = float(position.entry_price)
            entry_date = position.entry_date
            
            print(f"\n   📊 Checking {symbol} (Entry: ${entry_price:.2f} on {entry_date})")
            
            # Read data from DB batch (no API call)
            df = all_position_data.get(symbol)
            if df is None or len(df) < 50:
                print(f"      ⚠️  No data available for {symbol}")
                continue
            
            # Compute indicators
            df = compute_indicators(df)
            
            # Check exit conditions
            should_exit, exit_reason = check_exit_signals(df, symbol, entry_price, entry_date)
            
            current_price = df['close'].iloc[-1]
            profit_pct = ((current_price - entry_price) / entry_price) * 100
            
            if should_exit:
                print(f"      🚨 EXIT SIGNAL: {exit_reason}")
                print(f"      💰 Current: ${current_price:.2f} | P/L: {profit_pct:+.1f}%")
                exits.append((position, exit_reason, profit_pct))
            else:
                print(f"      ✅ HOLD: ${current_price:.2f} | P/L: {profit_pct:+.1f}%")
        
        # Remove exited positions from database
        if exits:
            print(f"\n📤 Exiting {len(exits)} positions:")
            for position, reason, profit_pct in exits:
                print(f"   • {position.symbol}: {reason} ({profit_pct:+.1f}%)")
                db_session.delete(position)
            
            db_session.commit()
            print(f"\n✅ Removed {len(exits)} positions from database")
        else:
            print(f"\n✅ All {len(positions)} positions remain active")
        
    except Exception as e:
        print(f"❌ Error checking positions: {e}")
        db_session.rollback()
    finally:
        db_session.close()


def scan_single_symbol_from_df(sym, df, method, slope_thresh, pct_thresh, lookback):
    """
    Screen a single symbol for Bora strategy criteria using a pre-loaded DataFrame.
    Pure synchronous — no API calls. Reads from DB data passed in.
    
    Args:
        sym: Stock symbol.
        df: Pre-loaded OHLCV DataFrame (from market_data DB table).
        method, slope_thresh, pct_thresh, lookback: Trend parameters.
    Returns:
        Symbol if it passes all filters, else None.
    """
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
    
    # Volatility & Risk Management Filters (mandatory)
    if not volatility_risk_ok(df):
        return None
    
    # Entry Timing Enhancement Filters (mandatory)
    if not entry_timing_ok(df, lookback=lookback):
        return None
    
    # Volume & Conviction Filters (optional - only if volume data available)
    has_volume_data = "volume" in df.columns and not df["volume"].isna().all()
    if has_volume_data:
        if not volume_conviction_ok(df, lookback=lookback):
            return None
    # If no volume data, we still proceed with trend+volatility+timing analysis

    return sym


async def scan_single_symbol(sym, session, method, slope_thresh, pct_thresh, lookback):
    """
    Legacy async wrapper — kept for backward compatibility.
    Delegates to scan_single_symbol_from_df using DB data.
    Falls back to API if DB has no data for this ticker.
    """
    df = get_dataframe_from_db(sym)
    if df is None or len(df) < 200:
        # Fallback to API if DB is empty
        df = await get_polygon_data(sym, session)
    return scan_single_symbol_from_df(sym, df, method, slope_thresh, pct_thresh, lookback)

async def scan_symbols(symbols, ema21_method="slope", slope_thresh=0.0, pct_thresh=1.0, lookback=10):
    """
    Run Bora strategy screen on a list of symbols.
    Now reads all data from the shared market_data DB table in one batch,
    then screens each symbol synchronously — no API calls needed.
    
    Falls back to API for any ticker missing from the DB.
    
    Args:
        symbols: List of stock symbols.
        ema21_method, slope_thresh, pct_thresh, lookback: Trend parameters.
    Returns:
        List of symbols that pass the Bora strategy.
    """
    import time
    t0 = time.time()
    
    # --- 1. Batch read all tickers from DB (single query, ~50ms for 500 tickers) ---
    print(f"📂 Loading market data from DB for {len(symbols)} tickers...")
    all_data = get_multiple_dataframes_from_db(list(symbols))
    db_hits = len(all_data)
    db_misses = len(symbols) - db_hits
    print(f"   ✅ DB: {db_hits} tickers loaded | ⚠️  Missing: {db_misses}")
    print(f"   ⏱️  DB batch read took {(time.time() - t0)*1000:.0f} ms")
    
    # --- 2. Screen each symbol synchronously using DB data ---
    t1 = time.time()
    picks = []
    api_fallback_needed = []
    
    for sym in symbols:
        df = all_data.get(sym)
        if df is not None and len(df) >= 200:
            result = scan_single_symbol_from_df(sym, df, ema21_method, slope_thresh, pct_thresh, lookback)
            if result:
                picks.append(result)
        elif df is None:
            api_fallback_needed.append(sym)
    
    print(f"   ⏱️  Screening {db_hits} tickers took {(time.time() - t1)*1000:.0f} ms")
    
    # --- 3. API fallback for missing tickers (if any) ---
    if api_fallback_needed:
        print(f"   🌐 Falling back to API for {len(api_fallback_needed)} missing tickers...")
        async with aiohttp.ClientSession() as session:
            tasks = [
                scan_single_symbol(sym, session, ema21_method, slope_thresh, pct_thresh, lookback)
                for sym in api_fallback_needed
            ]
            results = await asyncio.gather(*tasks)
            picks.extend([r for r in results if r])
    
    total_time = time.time() - t0
    print(f"   🏁 Total scan time: {total_time:.1f}s (vs ~90-120s before)")
    
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
        print(f"   • Risk Filters: ATR, Bollinger Bands, Beta proxy, Volatility stability")
        print(f"   • Timing Filters: Pullback bounce, Consolidation breakout, Gap continuation")
        print("🔍 Running comprehensive screening with optimal entry timing...")
        print("📂 Data source: Shared market_data DB table (centralized cache)")
        
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
        
        # Get entry prices from DB data (no additional API calls needed)
        print(f"\n📊 Reading entry prices from DB...")
        positions_data = []
        picks_data = get_multiple_dataframes_from_db(picks)
        for sym in picks:
            df = picks_data.get(sym)
            if df is not None and len(df) > 0:
                entry_price = df['close'].iloc[-1]
                # Calculate 6% stop loss and 20% target
                stop_loss = entry_price * 0.94
                target_price = entry_price * 1.20
                positions_data.append((sym, entry_price, stop_loss, target_price))
        
        # Add to BoraData (screening results - historical record)
        for i, sym in enumerate(picks, 1):
            entry = BoraData(
                symbol=sym,
                data_date=today,
                data_time=time_now,
                data_json=json.dumps({"Ticker": sym}),
            )
            session.add(entry)
            print(f"   💾 Added {sym} to BoraData ({i}/{len(picks)})")
        
        # Add to BoraPosition (active positions for exit monitoring)
        for sym, entry_price, stop_loss, target_price in positions_data:
            # Check if position already exists
            existing = session.query(BoraPosition).filter_by(symbol=sym).first()
            if existing:
                print(f"   ⚠️  {sym} already in positions - skipping")
                continue
            
            position = BoraPosition(
                symbol=sym,
                entry_date=today,
                entry_price=str(entry_price),
                stop_loss=str(stop_loss),
                target_price=str(target_price),
                entry_reason="Bora filters passed (Trend+Vol+Volume+Timing)"
            )
            session.add(position)
            print(f"   📈 Added {sym} position: Entry=${entry_price:.2f}, Stop=${stop_loss:.2f}, Target=${target_price:.2f}")
        
        session.commit()
        print(f"\n🎉 SUCCESS: Inserted {len(picks)} entries and {len(positions_data)} positions into DB")
        
    except Exception as e:
        print(f"❌ Error in run_and_store_bora: {e}")
        import traceback
        traceback.print_exc()
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