"""
Mark Minevari's VCP (Volatility Contraction Pattern) Scanner Strategy
Modified to use Polygon API instead of yfinance.

VCP Pattern Criteria:
1. Strong uptrend: Stock up 30%+ in last 60 days and above 150/200 day MA
2. Consolidation: Sideways movement for 3-12 weeks in tight range (<35%)
3. Contractions: 2-6 pullbacks with shrinking depth and higher lows
4. Volume drying up: Volume contracts during base formation
5. Breakout: Close above resistance with 40%+ volume spike

Original implementation by Mark Minevari
Polygon API integration by GitHub Copilot
"""

import pandas as pd
import numpy as np
import asyncio
import aiohttp
import sys
import os
import json
from datetime import datetime

# Add the parent directory to the path to import services
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from aignitequant.app.services.polygon import get_polygon_data
from aignitequant.app.services.sp500 import get_sp500_tickers, get_sector_map
from aignitequant.app.db import SessionLocal, VCPData
from aignitequant.app.services.market_data import get_dataframe_from_db, get_multiple_dataframes_from_db

# ---------- PARAMETERS ---------- 
# Adjusted to be more realistic for actual market conditions
MIN_UPTREND_PCT = 0.20 # stock up 20%+ in longer period (was too strict at 30%)
LOOKBACK_UPTREND_DAYS = 120 # look back 4 months instead of 2 (more realistic)
MIN_CONSOLIDATION_DAYS = 15 # 3+ weeks [web:11]
MAX_CONSOLIDATION_DAYS = 60 # 12 weeks [web:11]
MIN_CONTRACTIONS = 2 # 2–6 pullbacks [web:4]
BREAKOUT_VOL_MULT = 1.4 # 30–40%+ spike in volume [web:4]

def configure_vcp_parameters(min_uptrend_pct=None, lookback_days=None, 
                           min_consolidation_days=None, max_consolidation_days=None,
                           min_contractions=None, breakout_vol_mult=None):
    """
    Configure VCP scanning parameters.
    
    Args:
        min_uptrend_pct: Minimum uptrend percentage (default: 0.30)
        lookback_days: Days to look back for uptrend (default: 60)
        min_consolidation_days: Minimum consolidation period (default: 15)
        max_consolidation_days: Maximum consolidation period (default: 60)
        min_contractions: Minimum number of contractions (default: 2)
        breakout_vol_mult: Volume multiplier for breakout (default: 1.4)
    """
    global MIN_UPTREND_PCT, LOOKBACK_UPTREND_DAYS, MIN_CONSOLIDATION_DAYS
    global MAX_CONSOLIDATION_DAYS, MIN_CONTRACTIONS, BREAKOUT_VOL_MULT
    
    if min_uptrend_pct is not None:
        MIN_UPTREND_PCT = min_uptrend_pct
    if lookback_days is not None:
        LOOKBACK_UPTREND_DAYS = lookback_days
    if min_consolidation_days is not None:
        MIN_CONSOLIDATION_DAYS = min_consolidation_days
    if max_consolidation_days is not None:
        MAX_CONSOLIDATION_DAYS = max_consolidation_days
    if min_contractions is not None:
        MIN_CONTRACTIONS = min_contractions
    if breakout_vol_mult is not None:
        BREAKOUT_VOL_MULT = breakout_vol_mult
    
    print(f"VCP Parameters updated:")
    print(f"  Min uptrend: {MIN_UPTREND_PCT*100:.0f}%")
    print(f"  Lookback days: {LOOKBACK_UPTREND_DAYS}")
    print(f"  Consolidation range: {MIN_CONSOLIDATION_DAYS}-{MAX_CONSOLIDATION_DAYS} days")
    print(f"  Min contractions: {MIN_CONTRACTIONS}")
    print(f"  Breakout volume multiplier: {BREAKOUT_VOL_MULT}x")

# ---------- CORE FUNCTIONS ----------

async def get_data(symbol):
    """Download daily OHLCV data — DB first, API fallback."""
    try:
        # Try DB first
        df = get_dataframe_from_db(symbol)
        if df is not None and len(df) >= 200:
            # Rename columns to match yfinance format (capitalize first letter)
            df = df.rename(columns={
                'open': 'Open',
                'high': 'High', 
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            })
            df.dropna(inplace=True)
            return df

        # Fallback to API
        async with aiohttp.ClientSession() as session:
            df = await get_polygon_data(symbol, session)
            if df is None or df.empty:
                print(f"⚠️ No data returned for {symbol}")
                return None
            
            df = df.rename(columns={
                'open': 'Open',
                'high': 'High', 
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            })
            df.dropna(inplace=True)
            
            if len(df) < 200:
                print(f"⚠️ Insufficient data for {symbol}: only {len(df)} days")
                return None
                
            return df
            
    except Exception as e:
        print(f"❌ Error fetching data for {symbol}: {e}")
        return None

def in_strong_uptrend(df, as_of=-1, debug=False):
    """
    Improved uptrend detection based on VCP pattern image:
    1. Stock above 150/200 day moving averages (trend direction)
    2. Meaningful gain over longer period (not just 60 days)
    3. Recent consolidation after uptrend (not requiring immediate gain)
    """
    if len(df) < LOOKBACK_UPTREND_DAYS + 5:
        if debug:
            print(f"   ❌ Insufficient data: {len(df)} < {LOOKBACK_UPTREND_DAYS + 5}")
        return False

    recent_close = df["Close"].iloc[as_of]
    
    # Check if we have enough data for moving averages
    if len(df) < 200:
        if debug:
            print(f"   ❌ Insufficient data for 200-day MA: {len(df)} days")
        return False
        
    ma150 = df["Close"].rolling(150).mean().iloc[as_of]
    ma200 = df["Close"].rolling(200).mean().iloc[as_of]
    ma50 = df["Close"].rolling(50).mean().iloc[as_of]
    
    # Check for NaN values
    if pd.isna(ma150) or pd.isna(ma200) or pd.isna(ma50):
        if debug:
            print(f"   ❌ Moving averages are NaN: MA50={ma50}, MA150={ma150}, MA200={ma200}")
        return False

    # Multiple timeframe checks for more realistic uptrend detection
    past_close_120 = df["Close"].iloc[as_of - LOOKBACK_UPTREND_DAYS] if len(df) > LOOKBACK_UPTREND_DAYS else df["Close"].iloc[0]
    past_close_252 = df["Close"].iloc[as_of - 252] if len(df) > 252 else df["Close"].iloc[0]  # 1 year
    
    pct_gain_120 = (recent_close - past_close_120) / past_close_120
    pct_gain_252 = (recent_close - past_close_252) / past_close_252
    
    # Key criteria based on VCP image:
    above_ma150 = recent_close > ma150
    above_ma200 = recent_close > ma200
    above_ma50 = recent_close > ma50
    ma_trending_up = ma50 > ma150 > ma200  # MAs in proper order
    
    # At least one meaningful uptrend timeframe
    uptrend_4month = pct_gain_120 >= MIN_UPTREND_PCT
    uptrend_1year = pct_gain_252 >= (MIN_UPTREND_PCT * 2)  # 40%+ over 1 year is strong
    
    # Stock should be above key MAs and show uptrend in at least one timeframe
    uptrend_ok = (uptrend_4month or uptrend_1year) and above_ma150 and above_ma200
    
    if debug:
        print(f"   📈 4-month gain: {pct_gain_120*100:.1f}% (need {MIN_UPTREND_PCT*100:.0f}%+) - {'✅' if uptrend_4month else '❌'}")
        print(f"   📈 1-year gain: {pct_gain_252*100:.1f}% (need {MIN_UPTREND_PCT*2*100:.0f}%+) - {'✅' if uptrend_1year else '❌'}")
        print(f"   📈 Current: ${recent_close:.2f}, MA50: ${ma50:.2f}, MA150: ${ma150:.2f}, MA200: ${ma200:.2f}")
        print(f"   📈 Above MA50: {above_ma50} {'✅' if above_ma50 else '❌'}")
        print(f"   📈 Above MA150: {above_ma150} {'✅' if above_ma150 else '❌'}")
        print(f"   📈 Above MA200: {above_ma200} {'✅' if above_ma200 else '❌'}")
        print(f"   📈 MA trending up: {ma_trending_up} {'✅' if ma_trending_up else '❌'}")
        print(f"   📈 Overall uptrend: {'✅' if uptrend_ok else '❌'}")

    return uptrend_ok

def find_consolidation_window(df, debug=False):
    """
    Roughly detect a sideways consolidation:
    - price stays within a band
    - lasts 3–12 weeks
    """
    closes = df["Close"]
    best_start, best_end = None, None

    for length in range(MIN_CONSOLIDATION_DAYS, MAX_CONSOLIDATION_DAYS + 1):
        if length > len(df):
            continue
            
        window = closes.iloc[-length:]
        hi, lo = window.max(), window.min()
        rng = (hi - lo) / lo

        # require relatively tight range (e.g. < 35%)
        if rng < 0.35:
            best_start = len(df) - length
            best_end = len(df) - 1
            if debug:
                print(f"   📊 Found consolidation: {length} days, range: {rng*100:.1f}%")
            break

    if debug and best_start is None:
        print(f"   ❌ No consolidation found in {MIN_CONSOLIDATION_DAYS}-{MAX_CONSOLIDATION_DAYS} day range")
        print(f"   📊 Recent price ranges checked:")
        for length in [15, 30, 45, 60]:
            if length <= len(df):
                window = closes.iloc[-length:]
                hi, lo = window.max(), window.min()
                rng = (hi - lo) / lo
                print(f"      {length} days: {rng*100:.1f}% range ({'✅' if rng < 0.35 else '❌'})")

    return best_start, best_end

def detect_contractions(df, start, end):
    """
    Look for 2–6 pullbacks with shrinking depth and higher lows. [web:4][web:6]
    Very approximate: use swing highs/lows on closing prices.
    """
    closes = df["Close"].iloc[start:end+1]
    highs = df["High"].iloc[start:end+1]
    lows = df["Low"].iloc[start:end+1]

    # simple swing points
    swing_highs = []
    swing_lows = []

    for i in range(1, len(closes)-1):
        if highs.iloc[i] > highs.iloc[i-1] and highs.iloc[i] > highs.iloc[i+1]:
            swing_highs.append((closes.index[i], highs.iloc[i]))
        if lows.iloc[i] < lows.iloc[i-1] and lows.iloc[i] < lows.iloc[i+1]:
            swing_lows.append((closes.index[i], lows.iloc[i]))

    # align highs/lows into pullbacks
    swing_highs.sort()
    swing_lows.sort()

    contractions = []
    for i in range(min(len(swing_highs), len(swing_lows)) - 1):
        h_date, h_price = swing_highs[i]
        l_date, l_price = swing_lows[i]
        if l_date <= h_date:
            continue
        depth = (h_price - l_price) / h_price
        contractions.append((h_date, l_date, depth, h_price, l_price))

    if len(contractions) < MIN_CONTRACTIONS:
        return None

    # verify each depth smaller than previous and lows rising
    depths = [c[2] for c in contractions]
    lows_only = [c[4] for c in contractions]

    shrinking = all(depths[i] > depths[i+1] for i in range(len(depths)-1))
    higher_lows = all(lows_only[i] < lows_only[i+1] for i in range(len(lows_only)-1))

    if not (shrinking and higher_lows):
        return None

    return contractions

def volume_drying_up(df, start, end):
    """
    Volume should contract during the base, then spike on breakout. [web:3][web:4][web:6]
    """
    vol = df["Volume"]
    base_vol = vol.iloc[start:end+1]
    # simple check: average volume in last third < first third
    n = len(base_vol)
    if n < 9:
        return False

    first = base_vol.iloc[: n//3].mean()
    last = base_vol.iloc[- n//3 :].mean()

    return last < first

def breakout_signal(df, start, end):
    """
    Breakout = close above base resistance + volume spike. [web:4][web:8]
    """
    base = df.iloc[start:end+1]
    pivot = base["High"].max() # resistance
    recent = df.iloc[end+1:]

    if recent.empty:
        return None

    avg_vol = base["Volume"].mean()

    for idx, row in recent.iterrows():
        price_breakout = row["Close"] > pivot
        vol_spike = row["Volume"] > BREAKOUT_VOL_MULT * avg_vol
        if price_breakout and vol_spike:
            return {
                "date": idx,
                "pivot": pivot,
                "close": row["Close"],
                "volume": row["Volume"],
                "avg_base_volume": avg_vol
            }
    return None

async def scan_symbol_for_vcp(symbol, debug=False):
    """Scan a symbol for VCP pattern using Polygon API data."""
    if debug:
        print(f"\n🔍 Analyzing {symbol} for VCP pattern...")
    
    df = await get_data(symbol)
    
    if df is None:
        if debug:
            print(f"   ❌ No data available for {symbol}")
        return {"symbol": symbol, "VCP": False, "reason": "No data available"}

    if debug:
        print(f"   📊 Data available: {len(df)} days")
        print(f"   📊 Price range: ${df['Close'].min():.2f} - ${df['Close'].max():.2f}")
        print(f"   📊 Current price: ${df['Close'].iloc[-1]:.2f}")

    if not in_strong_uptrend(df, debug=debug):
        return {"symbol": symbol, "VCP": False, "reason": "No strong uptrend"}

    start, end = find_consolidation_window(df, debug=debug)
    if start is None:
        return {"symbol": symbol, "VCP": False, "reason": "No clear consolidation"}

    cons_contractions = detect_contractions(df, start, end)
    if cons_contractions is None:
        if debug:
            print(f"   ❌ No valid contractions found")
        return {"symbol": symbol, "VCP": False, "reason": "No valid contractions"}
    
    if debug:
        print(f"   ✅ Found {len(cons_contractions)} contractions")

    if not volume_drying_up(df, start, end):
        if debug:
            print(f"   ❌ Volume not drying up during consolidation")
        return {"symbol": symbol, "VCP": False, "reason": "Volume not drying up"}
    
    if debug:
        print(f"   ✅ Volume drying up during consolidation")

    breakout = breakout_signal(df, start, end)
    if breakout is None:
        if debug:
            print(f"   🔄 VCP pattern forming, waiting for breakout")
        return {"symbol": symbol, "VCP": True,
                "status": "Pattern forming, no breakout yet",
                "base_start": df.index[start],
                "base_end": df.index[end]}

    if debug:
        print(f"   🚀 BREAKOUT detected!")
    return {
        "symbol": symbol,
        "VCP": True,
        "status": "Breakout",
        "base_start": df.index[start],
        "base_end": df.index[end],
        "breakout_info": breakout
    }

async def scan_multiple_symbols(symbols, batch_size=5, delay=1.0):
    """
    Scan multiple symbols for VCP patterns concurrently with rate limiting.
    
    Args:
        symbols: List of ticker symbols to scan
        batch_size: Number of concurrent requests per batch
        delay: Delay between batches to respect API rate limits
    """
    results = []
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}: {batch}")
        
        tasks = [scan_symbol_for_vcp(symbol) for symbol in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for symbol, result in zip(batch, batch_results):
            if isinstance(result, Exception):
                print(f"Error scanning {symbol}: {result}")
                results.append({"symbol": symbol, "VCP": False, "reason": f"Error: {str(result)}"})
            else:
                results.append(result)
        
        # Wait between batches to respect API rate limits
        if i + batch_size < len(symbols):
            await asyncio.sleep(delay)
    
    return results

def get_vcp_candidates(results):
    """Filter results to show only VCP candidates."""
    return [r for r in results if r.get('VCP', False)]


def save_vcp_results_to_db(results):
    """
    Save VCP scanner results to database
    
    Args:
        results: List of VCP scan results from scan_sp500_for_vcp()
    
    Returns:
        Number of records saved to database
    """
    db = SessionLocal()
    try:
        now = datetime.now()
        today = now.date()
        time_now = now.time().replace(microsecond=0)
        
        saved_count = 0
        for result in results:
            # Only save VCP candidates and breakouts (not all stocks)
            if result.get('VCP') or result.get('breakout', False):
                vcp_entry = VCPData(
                    symbol=result['symbol'],
                    sector=result.get('sector', 'Unknown'),
                    status=result.get('status', 'VCP'),
                    data_date=today,
                    data_time=time_now,
                    data_json=json.dumps(result, default=str)
                )
                db.add(vcp_entry)
                saved_count += 1
        
        db.commit()
        print(f"\n💾 Database: Saved {saved_count} VCP candidates to database")
        return saved_count
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error saving to database: {e}")
        return 0
    finally:
        db.close()


async def list_sp500_sectors():
    """List all available S&P 500 sectors."""
    print("🔍 Fetching S&P 500 sector data...")
    sector_map = await get_sector_map()
    
    print(f"\n=== Available S&P 500 Sectors ===")
    for sector, tickers in sorted(sector_map.items()):
        print(f"📊 {sector}: {len(tickers)} companies")
        # Show a few example tickers
        examples = tickers[:5]
        print(f"   Examples: {', '.join(examples)}")
    
    return list(sector_map.keys())

async def scan_sp500_for_vcp(batch_size=10, delay=2.0):
    """
    Scan all S&P 500 stocks for VCP patterns.
    Uses DB batch read for speed.
    """
    import time
    print("Fetching S&P 500 tickers...")
    sp500_tickers = await get_sp500_tickers()
    print(f"Got {len(sp500_tickers)} S&P 500 tickers")
    
    # Pre-load all data from DB
    t0 = time.time()
    dfs = get_multiple_dataframes_from_db(sp500_tickers)
    db_hits = sum(1 for v in dfs.values() if v is not None and not v.empty)
    print(f"DB batch read: {db_hits}/{len(sp500_tickers)} tickers in {time.time()-t0:.2f}s")
    
    print(f"🚀 Starting VCP scan of S&P 500 (batch size: {batch_size}, delay: {delay}s)")
    results = await scan_multiple_symbols(sp500_tickers, batch_size=batch_size, delay=delay)
    
    return results

async def scan_sp500_by_sector(sector_name=None, batch_size=10, delay=2.0):
    """
    Scan S&P 500 stocks by sector for VCP patterns.
    
    Args:
        sector_name: Specific sector to scan (e.g., 'Technology'). If None, scan all sectors.
        batch_size: Number of concurrent requests per batch
        delay: Delay between batches to respect API rate limits
    """
    print("🔍 Fetching S&P 500 sector data...")
    sector_map = await get_sector_map()
    
    if sector_name:
        if sector_name not in sector_map:
            available_sectors = list(sector_map.keys())
            print(f"❌ Sector '{sector_name}' not found. Available sectors:")
            for sector in sorted(available_sectors):
                print(f"   - {sector}")
            return []
        
        tickers = sector_map[sector_name]
        print(f"✅ Scanning {len(tickers)} stocks in {sector_name} sector")
        results = await scan_multiple_symbols(tickers, batch_size=batch_size, delay=delay)
        return results
    else:
        all_results = []
        for sector, tickers in sector_map.items():
            print(f"🏢 Scanning {sector} sector ({len(tickers)} stocks)...")
            sector_results = await scan_multiple_symbols(tickers, batch_size=batch_size, delay=delay)
            
            # Add sector info to results
            for result in sector_results:
                result['sector'] = sector
            
            all_results.extend(sector_results)
            
            # Brief pause between sectors
            await asyncio.sleep(1.0)
        
        return all_results

def print_vcp_summary(results):
    """Print a summary of VCP scanning results."""
    total = len(results)
    vcp_count = len(get_vcp_candidates(results))
    
    print(f"\n=== VCP SCANNER SUMMARY ===")
    print(f"Total symbols scanned: {total}")
    print(f"VCP patterns found: {vcp_count}")
    
    if total > 0:
        print(f"Success rate: {vcp_count/total*100:.1f}%")
    else:
        print("Success rate: N/A (no symbols scanned)")
    
    if vcp_count > 0:
        print(f"\n=== VCP CANDIDATES ===")
        candidates = get_vcp_candidates(results)
        
        # Group by sector if available
        by_sector = {}
        for result in candidates:
            sector = result.get('sector', 'Unknown')
            if sector not in by_sector:
                by_sector[sector] = []
            by_sector[sector].append(result)
        
        for sector, sector_results in by_sector.items():
            if sector != 'Unknown':
                print(f"\n📊 {sector} Sector:")
            
            for result in sector_results:
                status = result.get('status', 'Unknown')
                symbol_display = f"📈 {result['symbol']}: {status}"
                if sector == 'Unknown':
                    print(symbol_display)
                else:
                    print(f"   {symbol_display}")
                
                if 'breakout_info' in result:
                    breakout = result['breakout_info']
                    indent = "     " if sector != 'Unknown' else "   "
                    print(f"{indent}Breakout: ${breakout['close']:.2f} on {breakout['date'].strftime('%Y-%m-%d')}")

# ---------- EXAMPLE USAGE ----------
async def main():
    # Example 0: List available sectors
    print("=== Available Sectors ===")
    sectors = await list_sp500_sectors()
    
    # Example 1: Single symbol with debug
    print("\n=== Single Symbol Scan (Debug) ===")
    symbol = "NVDA"  # put any ticker here
    result = await scan_symbol_for_vcp(symbol, debug=True)
    print(f"\n{symbol} Result: {result}")
    
    # Example 2: Full Information Technology sector scan (ALL tech stocks)
    print("\n=== FULL Information Technology Sector VCP Scan ===")
    tech_results = await scan_sp500_by_sector("Information Technology", batch_size=5, delay=1.0)
    print_vcp_summary(tech_results)  # This shows ALL tech stocks, not just a sample
    
    # Example 3: Full S&P 500 scan (uncomment to scan ALL 500 stocks)
    # WARNING: This takes 10-15 minutes due to API rate limits
    # print("\n=== FULL S&P 500 VCP Scan (ALL 500+ STOCKS) ===")
    # sp500_results = await scan_sp500_for_vcp(batch_size=10, delay=2.0)
    # print_vcp_summary(sp500_results)
    
    # Example 4: Quick sample scan of just a few stocks
    print("\n=== Quick Sample Scan (5 stocks) ===")
    symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]
    results = await scan_multiple_symbols(symbols)
    print_vcp_summary(results)

async def scan_sample_stocks():
    """Quick scan of popular stocks for testing."""
    popular_stocks = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "NFLX", 
        "AMD", "CRM", "ADBE", "ORCL", "NOW", "AVGO", "QCOM", "INTC"
    ]
    
    print(f"🔍 Quick VCP scan of {len(popular_stocks)} popular stocks...")
    results = await scan_multiple_symbols(popular_stocks, batch_size=8, delay=1.0)
    print_vcp_summary(results)
    return results

async def debug_scan_popular_stocks():
    """Debug scan of popular stocks to understand why no VCP patterns are found."""
    popular_stocks = ["AAPL", "MSFT", "GOOGL", "NVDA", "TSLA", "AMZN", "META", "NFLX"]
    
    print(f"\n🔍 DEBUG: Analyzing {len(popular_stocks)} popular stocks for VCP patterns...")
    print("=" * 80)
    
    failure_reasons = {}
    
    for symbol in popular_stocks:
        print(f"\n{'='*20} {symbol} {'='*20}")
        result = await scan_symbol_for_vcp(symbol, debug=True)
        
        reason = result.get('reason', 'Unknown')
        if reason not in failure_reasons:
            failure_reasons[reason] = 0
        failure_reasons[reason] += 1
        
        if result['VCP']:
            print(f"🎉 {symbol}: VCP PATTERN FOUND!")
        else:
            print(f"❌ {symbol}: {reason}")
    
    print(f"\n{'='*20} FAILURE ANALYSIS {'='*20}")
    for reason, count in failure_reasons.items():
        print(f"{reason}: {count} stocks")
    
    return failure_reasons

async def relaxed_vcp_scan(symbol):
    """More relaxed VCP criteria to see if we can find any patterns."""
    print(f"\n🔍 RELAXED scan for {symbol}...")
    
    # Temporarily relax parameters even further
    global MIN_UPTREND_PCT, MIN_CONSOLIDATION_DAYS, MAX_CONSOLIDATION_DAYS
    original_uptrend = MIN_UPTREND_PCT
    original_min_days = MIN_CONSOLIDATION_DAYS
    original_max_days = MAX_CONSOLIDATION_DAYS
    
    MIN_UPTREND_PCT = 0.10  # Relax to 10% (very lenient)
    MIN_CONSOLIDATION_DAYS = 8   # Very short consolidation
    MAX_CONSOLIDATION_DAYS = 120 # Much longer consolidation
    
    result = await scan_symbol_for_vcp(symbol, debug=True)
    
    # Restore original parameters
    MIN_UPTREND_PCT = original_uptrend
    MIN_CONSOLIDATION_DAYS = original_min_days
    MAX_CONSOLIDATION_DAYS = original_max_days
    
    return result

if __name__ == "__main__":
    print("🚀 Mark Minevari's VCP Scanner with Polygon API")
    print("=" * 50)
    print("RUNNING: FULL S&P 500 VCP SCAN")
    print("This will scan ALL 500+ S&P 500 stocks for VCP patterns")
    print("Using improved criteria: 20% in 4 months OR 40% in 1 year")
    print("Estimated time: 10-15 minutes")
    print("Results will be saved to timestamped JSON files")
    print("=" * 50)
    
    # Full S&P 500 scan with improved criteria
    async def full_scan():
        print("🔍 Starting comprehensive S&P 500 VCP analysis...")
        results = await scan_sp500_for_vcp(batch_size=10, delay=2.0)
        print_vcp_summary(results)
        
        # Save results to database
        saved_count = save_vcp_results_to_db(results)
        
        # Save results to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'sp500_vcp_results_{timestamp}.json'
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Full results saved to: {filename}")
        
        # Also save just the VCP candidates to a separate file
        vcp_candidates = get_vcp_candidates(results)
        if vcp_candidates:
            vcp_filename = f'vcp_candidates_{timestamp}.json'
            with open(vcp_filename, 'w') as f:
                json.dump(vcp_candidates, f, indent=2, default=str)
            print(f"💾 VCP candidates saved to: {vcp_filename}")
            print(f"💾 Database: Saved {saved_count} VCP candidates")
            
            # Print detailed VCP candidates with full information
            print(f"\n{'='*80}")
            print(f"🎯 DETAILED VCP CANDIDATES FOUND: {len(vcp_candidates)}")
            print(f"{'='*80}")
            
            for idx, candidate in enumerate(vcp_candidates, 1):
                symbol = candidate['symbol']
                status = candidate.get('status', 'VCP')
                sector = candidate.get('sector', 'N/A')
                
                print(f"\n{idx}. 📈 {symbol} - {sector}")
                print(f"   Status: {status}")
                
                # Print key metrics if available
                if 'current_price' in candidate:
                    print(f"   Current Price: ${candidate['current_price']:.2f}")
                
                if 'consolidation_days' in candidate:
                    print(f"   Consolidation Days: {candidate['consolidation_days']}")
                
                if 'contractions' in candidate:
                    print(f"   Contractions: {candidate['contractions']}")
                
                if 'uptrend_pct' in candidate:
                    print(f"   Uptrend Gain: {candidate['uptrend_pct']:.1%}")
                
                if 'volume_status' in candidate:
                    print(f"   Volume: {candidate['volume_status']}")
                
                if 'breakout_info' in candidate:
                    breakout = candidate['breakout_info']
                    print(f"   🚀 BREAKOUT: ${breakout['close']:.2f} on {breakout['date']}")
                    if 'volume_surge' in breakout:
                        print(f"      Volume Surge: {breakout['volume_surge']:.1%}")
                
                # Print reason if available
                if 'reason' in candidate and candidate.get('VCP'):
                    print(f"   ✅ {candidate['reason']}")
            
            print(f"\n{'='*80}")
            print(f"✨ Total VCP Candidates: {len(vcp_candidates)}")
            print(f"{'='*80}")
        else:
            print("\n⚠️ No VCP candidates found in S&P 500")
            print("   Consider reviewing criteria or market conditions")
        
        return results
    
    asyncio.run(full_scan())
    
    # Debug modes (commented out)
    # async def debug_analysis():
    #     failure_stats = await debug_scan_popular_stocks()
    #     print(f"\n{'='*20} RELAXED CRITERIA TEST {'='*20}")
    #     test_symbols = ["AAPL", "NVDA", "TSLA"]
    #     for symbol in test_symbols:
    #         relaxed_result = await relaxed_vcp_scan(symbol)
    #         print(f"Relaxed {symbol}: {'VCP FOUND' if relaxed_result['VCP'] else relaxed_result['reason']}")
    # asyncio.run(debug_analysis())