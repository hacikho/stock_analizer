"""
Vibia J. Hybrid Trading Strategy
---------------------------------
CANSLIM Individual Stocks + TQQQ Swing Trading System

This module implements the hybrid trading system used by Vibia J., a top performer in the 
US Investing Championship who achieved triple-digit returns. The strategy combines:

1. **CANSLIM Methodology for Individual Stocks:**
   - Screens IBD50, sector leaders, and IPO leaders
   - Demands 25%+ (preferably 30-35%+) quarterly earnings and sales growth
   - High EPS and RS ratings (above 95)
   - Increasing institutional support
   - Enters from Stage 1/2 bases (cup with handle, double bottoms)
   - Position sizing: 10% initial, up to 12.5-15% total per stock, 6-8 stocks total
   - Technical stops at 10-week/50-day MA (within 8%)

2. **TQQQ Swing Trading Strategy:**
   - Pivots to TQQQ when individual setups are scarce or market is choppy
   - Entry trigger: Nasdaq Composite pulls back 5-8%; then wait for 3 consecutive up days
   - Treats TQQQ chart like a growth stock (tracks levels/personality on TQQQ, not QQQ)
   - Stop loss rules (set at entry):
       a) Default: close below the low of the first up day
       b) V-shaped recovery / 21 EMA confluence: 2 closes below 21 EMA
          (if day 3 after 2 closes below 21 EMA is strong → hold; if weak → exit)
   - Can add remaining cash on follow-through day (day 4+ confirmation)
   - Position sizing: 25% initial core; let it grow beyond 50% if acting right
   - Sell into strength (partial, e.g. 10% of position) when:
       a) Price is 10-15%+ extended above the 21 EMA AND downside reversal off new high
       b) 2 closes below the 21 EMA (with day 3 confirmation check before full exit)
   - Target: reduce to ~50% when trimming; do NOT add when position is already oversized

**Core Philosophy:** 
Focus on true market leaders with potential to double/triple in 12-18 months.
Adapt between individual stocks and TQQQ based on market conditions.

Results are stored in the VibiaHybridData table in the database.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import json
import datetime
import pandas as pd
import numpy as np
import aiohttp
import asyncio
from typing import Dict, List, Optional, Tuple

from aignitequant.app.db import SessionLocal, VibiaHybridData
from aignitequant.app.services.polygon import get_polygon_data
from aignitequant.app.services.sp500 import get_sp500_tickers
from aignitequant.app.services.market_data import get_dataframe_from_db, get_multiple_dataframes_from_db


# ==================== CONFIGURATION ====================

# CANSLIM Stock Criteria
CANSLIM_MIN_QTR_EARNINGS_GROWTH = 0.25  # 25% minimum
CANSLIM_PREFERRED_QTR_EARNINGS_GROWTH = 0.30  # 30-35% preferred
CANSLIM_EXCEPTIONAL_EARNINGS_GROWTH = 0.70  # 70%+ exceptional
CANSLIM_MIN_SALES_GROWTH = 0.25  # 25% minimum
CANSLIM_MIN_EPS_RATING = 95  # Above 95
CANSLIM_MIN_RS_RATING = 95  # Above 95
CANSLIM_MIN_INSTITUTIONAL_HOLDERS = 3  # Minimum institutional holders
CANSLIM_INITIAL_POSITION_SIZE = 0.10  # 10% of portfolio
CANSLIM_MAX_POSITION_SIZE = 0.15  # 15% maximum per stock
CANSLIM_MAX_STOCKS = 8  # Hold 6-8 stocks
CANSLIM_STOP_LOSS_PCT = 0.08  # 8% from MA

# TQQQ Trading Criteria
# Note: pullback % is measured on the Nasdaq Composite (underlying), not TQQQ itself.
# 5-8% Nasdaq pullback ≈ 15-24% TQQQ pullback due to 3x leverage.
TQQQ_NASDAQ_MIN_PULLBACK_PCT = 0.05   # 5% Nasdaq pullback minimum trigger
TQQQ_NASDAQ_MAX_PULLBACK_PCT = 0.08   # 8% Nasdaq pullback maximum trigger
TQQQ_CONSECUTIVE_UP_DAYS_ENTRY = 3    # 3 consecutive up days off lows required before entry
TQQQ_INITIAL_POSITION_SIZE = 0.25     # 25% of portfolio (core position)
TQQQ_MAX_POSITION_SIZE = 0.50         # Reduce back toward 50% when trimming into strength
TQQQ_CORE_POSITION_SIZE = 0.25        # 25% core in Stage 2 uptrend
TQQQ_PROFIT_TARGET_PCT = 0.20         # 20% profit target (rough guide)
TQQQ_21EMA_EXTENSION_SELL_MIN = 0.10  # 10% above 21 EMA → consider trimming
TQQQ_21EMA_EXTENSION_SELL_MAX = 0.15  # 15%+ above 21 EMA → stronger sell signal
TQQQ_21EMA_CLOSES_BELOW_EXIT = 2      # 2 consecutive closes below 21 EMA triggers review
TQQQ_DISTRIBUTION_DAYS_THRESHOLD = 4  # 4-5 distribution days
TQQQ_BULLS_VS_BEARS_THRESHOLD = 60    # Above 60%


# ==================== HELPER FUNCTIONS ====================

def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Relative Strength Index."""
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def detect_base_pattern(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Detect if stock is forming a proper base (cup with handle, double bottom, etc.).
    
    Returns:
        Tuple of (base_detected, base_type)
    """
    if len(df) < 50:
        return False, "insufficient_data"
    
    # Look for consolidation period (trading in a range)
    lookback = df.tail(50)
    high = lookback['high'].max()
    low = lookback['low'].min()
    range_pct = (high - low) / low
    
    # Base should be relatively tight (not too volatile)
    if range_pct > 0.35:  # More than 35% range is too wide
        return False, "too_volatile"
    
    # Check if making higher lows (sign of accumulation)
    recent_lows = lookback['low'].tail(20)
    first_half_low = recent_lows.iloc[:10].min()
    second_half_low = recent_lows.iloc[10:].min()
    higher_lows = second_half_low > first_half_low
    
    # Check if near 52-week high
    week_52_high = df.tail(252)['high'].max() if len(df) >= 252 else df['high'].max()
    current_price = df['close'].iloc[-1]
    near_high = current_price >= (week_52_high * 0.90)  # Within 10% of 52-week high
    
    if higher_lows and near_high:
        return True, "proper_base"
    
    return False, "no_base"


def check_ma_support(df: pd.DataFrame, ma_period: int = 50) -> Tuple[bool, float]:
    """
    Check if stock is retaking or bouncing off moving average.
    
    Returns:
        Tuple of (at_ma_support, distance_from_ma_pct)
    """
    if len(df) < ma_period:
        return False, 0
    
    ma = df['close'].rolling(window=ma_period).mean().iloc[-1]
    current_price = df['close'].iloc[-1]
    
    distance_pct = (current_price - ma) / ma
    
    # At support if within 5% of MA (above or slightly below)
    at_support = -0.05 <= distance_pct <= 0.05
    
    return at_support, distance_pct * 100


def detect_stage(df: pd.DataFrame) -> int:
    """
    Detect Weinstein Stage (1=Base, 2=Advancing, 3=Top, 4=Declining).
    
    Returns:
        Stage number (1-4)
    """
    if len(df) < 200:
        return 0  # Unknown
    
    sma_50 = df['close'].rolling(window=50).mean().iloc[-1]
    sma_200 = df['close'].rolling(window=200).mean().iloc[-1]
    current_price = df['close'].iloc[-1]
    
    # Stage 2: Price above both MAs, 50-day above 200-day, MAs rising
    if current_price > sma_50 > sma_200:
        sma_50_slope = (df['close'].rolling(window=50).mean().iloc[-1] - 
                        df['close'].rolling(window=50).mean().iloc[-10]) / 10
        if sma_50_slope > 0:
            return 2  # Stage 2 Advancing
    
    # Stage 1: Price near or above 200-day MA, consolidating
    if current_price > sma_200 * 0.95 and abs(current_price - sma_200) / sma_200 < 0.10:
        return 1  # Stage 1 Base
    
    # Stage 4: Price below both MAs
    if current_price < sma_50 < sma_200:
        return 4  # Stage 4 Declining
    
    # Stage 3: Price near highs but losing momentum
    return 3  # Stage 3 Top


def count_distribution_days(df: pd.DataFrame, days: int = 25) -> int:
    """
    Count distribution days (price down on higher volume) in the last N days.
    
    Returns:
        Number of distribution days
    """
    if len(df) < days + 1:
        return 0
    
    recent_df = df.tail(days)
    distribution_count = 0
    
    for i in range(1, len(recent_df)):
        price_down = recent_df['close'].iloc[i] < recent_df['close'].iloc[i-1]
        volume_up = recent_df['volume'].iloc[i] > recent_df['volume'].iloc[i-1]
        
        if price_down and volume_up:
            distribution_count += 1
    
    return distribution_count


def check_volume_on_high(df: pd.DataFrame) -> bool:
    """
    Check if volume is declining on new highs (bearish sign).
    
    Returns:
        True if declining volume on new highs
    """
    if len(df) < 20:
        return False
    
    recent_df = df.tail(20)
    
    # Find recent highs
    highest_high = recent_df['high'].max()
    high_indices = recent_df[recent_df['high'] >= highest_high * 0.99].index
    
    if len(high_indices) < 2:
        return False
    
    # Check if volume is declining at these highs
    volumes = [recent_df.loc[idx, 'volume'] for idx in high_indices[-2:]]
    return volumes[-1] < volumes[0]


# ==================== CANSLIM STOCK SCREENING ====================

async def get_earnings_growth(symbol: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Get quarterly and annual earnings growth (placeholder - would integrate with financial API).
    
    Returns:
        Tuple of (quarterly_growth, annual_growth) as percentages
    """
    # TODO: Integrate with yfinance or financial API
    # For now, return None to indicate data not available
    # In production, this would fetch actual earnings data
    return None, None


async def get_institutional_data(symbol: str) -> Dict:
    """
    Get institutional ownership data (placeholder - would integrate with financial API).
    
    Returns:
        Dict with institutional holder count and changes
    """
    # TODO: Integrate with yfinance or institutional data API
    return {
        "holder_count": 0,
        "increasing": False
    }


def scan_canslim_stock_from_df(symbol: str, df: pd.DataFrame) -> Optional[Dict]:
    """Sync version: Scan a pre-loaded DataFrame for CANSLIM stock setup."""
    try:
        if df is None or len(df) < 200:
            return None
        df = df.sort_index()
        df['sma_50'] = df['close'].rolling(window=50).mean()
        df['sma_200'] = df['close'].rolling(window=200).mean()
        df['volume_ma'] = df['volume'].rolling(window=50).mean()
        current_price = df['close'].iloc[-1]
        stage = detect_stage(df)
        if stage not in [1, 2]:
            return None
        has_base, base_type = detect_base_pattern(df)
        if not has_base:
            return None
        at_ma_50, distance_50 = check_ma_support(df, 50)
        at_ma_10, distance_10 = check_ma_support(df, 10)
        if not at_ma_50 and distance_50 < -5:
            return None
        recent_lows = df['low'].tail(20)
        higher_lows = recent_lows.iloc[-1] > recent_lows.iloc[:10].min()
        returns_90d = (df['close'].iloc[-1] - df['close'].iloc[-90]) / df['close'].iloc[-90] if len(df) >= 90 else 0
        entry_signal = has_base and higher_lows and current_price >= df['sma_50'].iloc[-1] and stage in [1, 2]
        if not entry_signal:
            return None
        stop_loss = df['sma_50'].iloc[-1] * (1 - CANSLIM_STOP_LOSS_PCT)
        risk_pct = (current_price - stop_loss) / current_price
        signal = {
            "symbol": symbol, "strategy": "canslim_stock", "signal_type": "buy",
            "timestamp": datetime.datetime.now().isoformat(),
            "entry_price": float(current_price), "stop_loss": float(stop_loss),
            "stop_loss_pct": float(risk_pct * 100), "stage": int(stage),
            "base_type": base_type, "distance_from_50d_ma": float(distance_50),
            "distance_from_10d_ma": float(distance_10), "higher_lows": bool(higher_lows),
            "above_sma_50": bool(current_price > df['sma_50'].iloc[-1]),
            "above_sma_200": bool(current_price > df['sma_200'].iloc[-1]),
            "returns_90d": float(returns_90d * 100),
            "position_sizing": {"initial": f"{CANSLIM_INITIAL_POSITION_SIZE*100:.0f}%", "max": f"{CANSLIM_MAX_POSITION_SIZE*100:.0f}%", "recommendation": "Start with 10%, can add to 12.5-15% on bounces"},
            "quarterly_earnings_growth": None, "annual_earnings_growth": None,
            "institutional_holders": 0, "institutional_increasing": False,
            "target": "Double to triple in 12-18 months",
        }
        print(f"✅ CANSLIM STOCK: {symbol} @ ${current_price:.2f} | Stage {stage} | Base: {base_type}")
        return signal
    except Exception as e:
        print(f"Error scanning {symbol} for CANSLIM: {e}")
        return None


async def scan_canslim_stock(symbol: str, session: aiohttp.ClientSession) -> Optional[Dict]:
    """
    Scan for CANSLIM stock setup opportunities.
    
    Args:
        symbol: Stock symbol to scan
        session: aiohttp session
        
    Returns:
        Dict with signal details if criteria met, None otherwise
    """
    try:
        df = await get_polygon_data(symbol, session)
        if df is None or len(df) < 200:
            return None
        
        df = df.sort_index()
        
        # Calculate technical indicators
        df['sma_50'] = df['close'].rolling(window=50).mean()
        df['sma_200'] = df['close'].rolling(window=200).mean()
        df['volume_ma'] = df['volume'].rolling(window=50).mean()
        
        current_price = df['close'].iloc[-1]
        
        # Check stage
        stage = detect_stage(df)
        if stage not in [1, 2]:  # Only Stage 1 (base) or Stage 2 (advancing)
            return None
        
        # Check for base pattern
        has_base, base_type = detect_base_pattern(df)
        if not has_base:
            return None
        
        # Check MA support
        at_ma_50, distance_50 = check_ma_support(df, 50)
        at_ma_10, distance_10 = check_ma_support(df, 10)
        
        # Must be retaking or bouncing off 50-day MA
        if not at_ma_50 and distance_50 < -5:  # Too far below 50-day MA
            return None
        
        # Check for higher lows
        recent_lows = df['low'].tail(20)
        higher_lows = recent_lows.iloc[-1] > recent_lows.iloc[:10].min()
        
        # Get fundamental data (placeholder)
        qtr_growth, annual_growth = await get_earnings_growth(symbol)
        inst_data = await get_institutional_data(symbol)
        
        # Calculate relative strength (simplified)
        returns_90d = (df['close'].iloc[-1] - df['close'].iloc[-90]) / df['close'].iloc[-90] if len(df) >= 90 else 0
        
        # Entry signal: Stock emerging from base, at or above 50-day MA
        entry_signal = (
            has_base and
            higher_lows and
            current_price >= df['sma_50'].iloc[-1] and
            stage in [1, 2]
        )
        
        if not entry_signal:
            return None
        
        # Calculate position sizing and stops
        stop_loss = df['sma_50'].iloc[-1] * (1 - CANSLIM_STOP_LOSS_PCT)
        risk_pct = (current_price - stop_loss) / current_price
        
        signal = {
            "symbol": symbol,
            "strategy": "canslim_stock",
            "signal_type": "buy",
            "timestamp": datetime.datetime.now().isoformat(),
            "entry_price": float(current_price),
            "stop_loss": float(stop_loss),
            "stop_loss_pct": float(risk_pct * 100),
            "stage": int(stage),
            "base_type": base_type,
            "distance_from_50d_ma": float(distance_50),
            "distance_from_10d_ma": float(distance_10),
            "higher_lows": bool(higher_lows),
            "above_sma_50": bool(current_price > df['sma_50'].iloc[-1]),
            "above_sma_200": bool(current_price > df['sma_200'].iloc[-1]),
            "returns_90d": float(returns_90d * 100),
            "position_sizing": {
                "initial": f"{CANSLIM_INITIAL_POSITION_SIZE*100:.0f}%",
                "max": f"{CANSLIM_MAX_POSITION_SIZE*100:.0f}%",
                "recommendation": "Start with 10%, can add to 12.5-15% on bounces"
            },
            "quarterly_earnings_growth": float(qtr_growth) if qtr_growth else None,
            "annual_earnings_growth": float(annual_growth) if annual_growth else None,
            "institutional_holders": int(inst_data.get("holder_count", 0)),
            "institutional_increasing": bool(inst_data.get("increasing", False)),
            "target": "Double to triple in 12-18 months",
        }
        
        print(f"✅ CANSLIM STOCK: {symbol} @ ${current_price:.2f} | Stage {stage} | Base: {base_type}")
        return signal
        
    except Exception as e:
        print(f"Error scanning {symbol} for CANSLIM: {e}")
        return None


# ==================== TQQQ SWING TRADING ====================

def count_consecutive_up_days(df: pd.DataFrame, n: int = 3) -> int:
    """Count how many consecutive up-close days have occurred ending on the last bar."""
    count = 0
    closes = df['close'].values
    for i in range(len(closes) - 1, 0, -1):
        if closes[i] > closes[i - 1]:
            count += 1
        else:
            break
    return count


def scan_tqqq_entry_from_df(df: pd.DataFrame) -> Optional[Dict]:
    """
    Sync version: Scan pre-loaded TQQQ DataFrame for entry opportunities.

    Entry rules (from Vibha's transcript):
    - Wait for Nasdaq Composite to pull back 5-8% (approximated here via TQQQ pullback).
    - Require 3 consecutive up-close days off the recent low (signal of near-term bottom).
    - Price should be near or just above the 21 EMA at entry (convergence = tighter stop).
    - Stop loss: close below the low of the first up day (default), or
                 2 consecutive closes below 21 EMA if in a V-shaped recovery.
    - Position: start at 25% of portfolio; can add remaining cash on follow-through day.
    """
    try:
        symbol = "TQQQ"
        if df is None or len(df) < 100:
            return None
        df = df.sort_index()
        df['ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['sma_50'] = df['close'].rolling(window=50).mean()
        df['sma_10'] = df['close'].rolling(window=10).mean()

        current_price = df['close'].iloc[-1]
        ema_21 = df['ema_21'].iloc[-1]

        # Pullback: measured from recent 30-day high (proxy for Nasdaq 5-8% → TQQQ ~15-24%)
        recent_high = df['high'].tail(30).max()
        pullback_pct = (recent_high - current_price) / recent_high

        # 3 consecutive up days off the lows
        consec_up = count_consecutive_up_days(df)
        three_up_days = consec_up >= TQQQ_CONSECUTIVE_UP_DAYS_ENTRY

        # Near 21 EMA at entry (within 5%)
        dist_from_21ema = (current_price - ema_21) / ema_21
        near_21ema = abs(dist_from_21ema) <= 0.05

        # Stage check
        stage = detect_stage(df)

        # Entry requires: meaningful pullback + 3 up days + near 21 EMA + Stage 2
        # Pullback threshold on TQQQ ≈ 15-24% (3x of 5-8% Nasdaq)
        entry_signal = (
            pullback_pct >= 0.15 and
            three_up_days and
            (near_21ema or dist_from_21ema <= 0.10) and  # allow up to 10% above 21 EMA
            stage == 2
        )

        if not entry_signal:
            return None

        # Stop loss: low of the first up day (3 days ago relative to current bar)
        first_up_day_low = float(df['low'].iloc[-TQQQ_CONSECUTIVE_UP_DAYS_ENTRY])
        stop_loss_default = first_up_day_low

        # Alternative stop: 2 closes below 21 EMA (use when in V-shaped recovery)
        stop_loss_21ema = float(ema_21)  # exit on 2nd close below this level

        signal = {
            "symbol": "TQQQ",
            "strategy": "tqqq_swing",
            "signal_type": "buy",
            "timestamp": datetime.datetime.now().isoformat(),
            "entry_price": float(current_price),
            "ema_21": float(ema_21),
            "dist_from_21ema_pct": float(dist_from_21ema * 100),
            "stop_loss_default": stop_loss_default,
            "stop_loss_21ema_2closes": stop_loss_21ema,
            "stop_loss_note": "Default: close below low of 1st up day. V-shape: 2 closes below 21 EMA (hold if day 3 strong, exit if weak).",
            "pullback_from_high_pct": float(pullback_pct * 100),
            "recent_high": float(recent_high),
            "consecutive_up_days": int(consec_up),
            "near_21ema": bool(near_21ema),
            "stage": int(stage),
            "position_sizing": {
                "initial": f"{TQQQ_INITIAL_POSITION_SIZE*100:.0f}%",
                "max": f"{TQQQ_MAX_POSITION_SIZE*100:.0f}%",
                "core": f"{TQQQ_CORE_POSITION_SIZE*100:.0f}%",
                "recommendation": "Enter 25% on 3rd up day. Add remaining cash on follow-through day (day 4+). Let it grow; trim back toward 50% when selling into strength.",
            },
            "target": f"{TQQQ_PROFIT_TARGET_PCT*100:.0f}%+ profit",
            "expected_return": "20-25%+ swing; treat as position trade, not quick flip.",
        }
        print(f"✅ TQQQ ENTRY: {symbol} @ ${current_price:.2f} | Pullback: {pullback_pct*100:.1f}% | {consec_up} up days | 21EMA dist: {dist_from_21ema*100:.1f}% | Stage {stage}")
        return signal
    except Exception as e:
        print(f"Error scanning TQQQ for entry: {e}")
        return None


def scan_tqqq_exit_from_df(df: pd.DataFrame) -> Optional[Dict]:
    """
    Sync version: Scan pre-loaded TQQQ DataFrame for exit signals.

    Sell rules (from Vibha's transcript):
    PRIMARY — sell INTO STRENGTH (partial, ~10% of position per signal):
      1. Price is 10-15%+ extended above the 21 EMA AND there's a downside reversal off a new high.
      2. If already oversized (e.g. >50% of portfolio), do NOT add; look to trim.

    STOP / DEFENSIVE EXIT:
      3. 2 consecutive closes below the 21 EMA → review position.
         - If day 3 (the next day) is strong, hold.
         - If day 3 is weak, begin exiting.
      4. Close below the low of the first up day (default stop set at entry).

    SECONDARY signals (support the decision to trim):
      5. Declining volume on new highs.
      6. 4+ distribution days in last 25 days.
      7. 3 consecutive down days on rising volume.
    """
    try:
        symbol = "TQQQ"
        if df is None or len(df) < 100:
            return None
        df = df.sort_index()
        df['ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
        df['sma_10'] = df['close'].rolling(window=10).mean()
        df['volume_ma'] = df['volume'].rolling(window=50).mean()

        current_price = df['close'].iloc[-1]
        ema_21 = df['ema_21'].iloc[-1]
        dist_from_21ema = (current_price - ema_21) / ema_21  # positive = above

        exit_signals = []

        # --- PRIMARY: 21 EMA extension + downside reversal ---
        extended_above_21ema = dist_from_21ema >= TQQQ_21EMA_EXTENSION_SELL_MIN
        highly_extended = dist_from_21ema >= TQQQ_21EMA_EXTENSION_SELL_MAX
        # Downside reversal: closed in lower half of bar's range after making new high
        day_range = df['high'].iloc[-1] - df['low'].iloc[-1]
        close_position = (df['close'].iloc[-1] - df['low'].iloc[-1]) / day_range if day_range > 0 else 0.5
        recent_high = df['high'].tail(10).max()
        made_new_high_today = df['high'].iloc[-1] >= recent_high * 0.99
        downside_reversal = made_new_high_today and close_position < 0.4
        if extended_above_21ema and downside_reversal:
            exit_signals.append(f"21ema_extension_{dist_from_21ema*100:.1f}pct_plus_downside_reversal")
        if highly_extended:
            exit_signals.append(f"highly_extended_{dist_from_21ema*100:.1f}pct_above_21ema")

        # --- DEFENSIVE: 2 closes below 21 EMA ---
        closes_below_21ema = sum(
            1 for i in [-1, -2]
            if df['close'].iloc[i] < df['ema_21'].iloc[i]
        )
        day3_is_weak = (closes_below_21ema >= TQQQ_21EMA_CLOSES_BELOW_EXIT and
                        df['close'].iloc[-1] < df['close'].iloc[-2])
        day3_is_strong = (closes_below_21ema >= TQQQ_21EMA_CLOSES_BELOW_EXIT and
                          df['close'].iloc[-1] > df['close'].iloc[-2])
        if closes_below_21ema >= TQQQ_21EMA_CLOSES_BELOW_EXIT:
            if day3_is_weak:
                exit_signals.append(f"2_closes_below_21ema_day3_weak_EXIT")
            elif day3_is_strong:
                exit_signals.append(f"2_closes_below_21ema_day3_strong_HOLD")

        # --- SECONDARY signals ---
        if check_volume_on_high(df):
            exit_signals.append("declining_volume_on_highs")

        dist_days = count_distribution_days(df, 25)
        if dist_days >= TQQQ_DISTRIBUTION_DAYS_THRESHOLD:
            exit_signals.append(f"{dist_days}_distribution_days")

        last_3 = df.tail(3)
        three_down = all(last_3['close'].diff().dropna() < 0)
        vols_rising = all(last_3['volume'].diff().dropna() > 0)
        if three_down and vols_rising:
            exit_signals.append("3_down_days_rising_volume")

        below_10d_ma = current_price < df['sma_10'].iloc[-1]
        volume_rising = df['volume'].iloc[-1] > df['volume_ma'].iloc[-1]
        poor_close = close_position < 0.3
        if below_10d_ma and volume_rising and poor_close:
            exit_signals.append("poor_close_below_10d_ma")

        # Estimate profit from recent swing low
        assumed_entry = df['low'].tail(30).min()
        profit_pct = (current_price - assumed_entry) / assumed_entry

        # Trigger exit signal if any primary signal hit, or 3+ secondary signals
        primary_hit = any(
            s for s in exit_signals
            if "21ema_extension" in s or "2_closes_below_21ema_day3_weak" in s or "highly_extended" in s
        )
        exit_criteria_met = primary_hit or len(exit_signals) >= 3

        if not exit_criteria_met:
            return None

        # Determine action
        if "2_closes_below_21ema_day3_weak_EXIT" in exit_signals:
            action = "BEGIN EXITING — 2 closes below 21 EMA, day 3 weak"
            trim_pct = "Begin full exit, watch day 3"
        elif "2_closes_below_21ema_day3_strong_HOLD" in exit_signals:
            action = "HOLD — 2 closes below 21 EMA but day 3 strong"
            trim_pct = "Hold position, re-evaluate next close"
        elif highly_extended:
            action = "TRIM INTO STRENGTH — highly extended above 21 EMA"
            trim_pct = "Sell ~10% of position"
        else:
            action = "CONSIDER TRIMMING — sell into strength"
            trim_pct = "Sell ~10% of position on strength"

        signal = {
            "symbol": "TQQQ",
            "strategy": "tqqq_swing",
            "signal_type": "sell",
            "timestamp": datetime.datetime.now().isoformat(),
            "current_price": float(current_price),
            "ema_21": float(ema_21),
            "dist_from_21ema_pct": float(dist_from_21ema * 100),
            "exit_signals": exit_signals,
            "exit_signals_count": len(exit_signals),
            "closes_below_21ema": int(closes_below_21ema),
            "day3_is_weak": bool(day3_is_weak),
            "day3_is_strong": bool(day3_is_strong),
            "profit_pct": float(profit_pct * 100),
            "distribution_days": int(dist_days),
            "action": action,
            "trim_recommendation": trim_pct,
            "note": "Never add to position when already oversized (>50% portfolio). Trim back toward 50% when selling into strength.",
        }
        print(f"⚠️ TQQQ EXIT: {symbol} @ ${current_price:.2f} | 21EMA dist: {dist_from_21ema*100:.1f}% | Signals: {exit_signals} | Profit: {profit_pct*100:.1f}%")
        return signal
    except Exception as e:
        print(f"Error scanning TQQQ for exit: {e}")
        return None


async def scan_tqqq_entry(session: aiohttp.ClientSession) -> Optional[Dict]:
    """Async wrapper: fetch TQQQ data then delegate to sync scan_tqqq_entry_from_df."""
    try:
        df = await get_polygon_data("TQQQ", session)
        return scan_tqqq_entry_from_df(df)
    except Exception as e:
        print(f"Error scanning TQQQ for entry: {e}")
        return None


async def scan_tqqq_exit(session: aiohttp.ClientSession) -> Optional[Dict]:
    """Async wrapper: fetch TQQQ data then delegate to sync scan_tqqq_exit_from_df."""
    try:
        df = await get_polygon_data("TQQQ", session)
        return scan_tqqq_exit_from_df(df)
    except Exception as e:
        print(f"Error scanning TQQQ for exit: {e}")
        return None


# ==================== MARKET CONDITION ASSESSMENT ====================

def assess_market_condition_from_df(spy_df: pd.DataFrame) -> Dict:
    """Sync version: Assess market conditions from a pre-loaded SPY DataFrame."""
    try:
        if spy_df is None or len(spy_df) < 50:
            return {"recommendation": "unknown", "reason": "insufficient_data"}
        spy_df = spy_df.sort_index()
        dist_days = count_distribution_days(spy_df, 25)
        market_stage = detect_stage(spy_df)
        if dist_days <= 2 and market_stage == 2:
            recommendation, reason = "individual_stocks", "Strong market with minimal distribution, focus on individual stock setups"
        elif dist_days >= 4 or market_stage in [3, 4]:
            recommendation, reason = "tqqq_or_cash", "Market showing weakness, pivot to TQQQ swing trading or cash"
        elif market_stage == 1:
            recommendation, reason = "tqqq_early_entry", "Market emerging from correction, use TQQQ for early entry"
        else:
            recommendation, reason = "flexible", "Mixed market conditions, be selective with both approaches"
        return {"recommendation": recommendation, "reason": reason, "distribution_days": int(dist_days), "market_stage": int(market_stage), "timestamp": datetime.datetime.now().isoformat()}
    except Exception as e:
        return {"recommendation": "unknown", "reason": str(e)}


async def assess_market_condition(session: aiohttp.ClientSession) -> Dict:
    """
    Assess overall market conditions to decide between individual stocks and TQQQ.
    
    Returns:
        Dict with market assessment and recommendation
    """
    try:
        # Get SPY (market proxy) data
        spy_df = await get_polygon_data("SPY", session)
        
        if spy_df is None or len(spy_df) < 50:
            return {"recommendation": "unknown", "reason": "insufficient_data"}
        
        spy_df = spy_df.sort_index()
        
        # Count distribution days
        dist_days = count_distribution_days(spy_df, 25)
        
        # Check market stage
        market_stage = detect_stage(spy_df)
        
        # Determine recommendation
        if dist_days <= 2 and market_stage == 2:
            recommendation = "individual_stocks"
            reason = "Strong market with minimal distribution, focus on individual stock setups"
        elif dist_days >= 4 or market_stage in [3, 4]:
            recommendation = "tqqq_or_cash"
            reason = "Market showing weakness, pivot to TQQQ swing trading or cash"
        elif market_stage == 1:
            recommendation = "tqqq_early_entry"
            reason = "Market emerging from correction, use TQQQ for early entry"
        else:
            recommendation = "flexible"
            reason = "Mixed market conditions, be selective with both approaches"
        
        return {
            "recommendation": recommendation,
            "reason": reason,
            "distribution_days": int(dist_days),
            "market_stage": int(market_stage),
            "timestamp": datetime.datetime.now().isoformat(),
        }
        
    except Exception as e:
        print(f"Error assessing market condition: {e}")
        return {"recommendation": "unknown", "reason": str(e)}


# ==================== MAIN SCREENING FUNCTIONS ====================

async def run_vibia_hybrid_screen(symbols: List[str]) -> Dict[str, List[Dict]]:
    """
    Run Vibia J.'s hybrid strategy screen.
    
    Args:
        symbols: List of stock symbols to scan
        
    Returns:
        Dict with strategy results
    """
    results = {
        "canslim_stocks": [],
        "tqqq_entry": None,
        "tqqq_exit": None,
        "market_assessment": None
    }
    
    # --- DB batch read: load all needed tickers at once ---
    import time
    all_needed = list(symbols) + ["TQQQ", "SPY"]
    t0 = time.time()
    dfs = get_multiple_dataframes_from_db(all_needed)
    db_hits = sum(1 for v in dfs.values() if v is not None and not v.empty)
    print(f"📂 DB batch read: {db_hits}/{len(all_needed)} tickers in {time.time()-t0:.2f}s")

    print(f"🔍 Running Vibia J. Hybrid Strategy Screen...")
    print(f"📊 Scanning {len(symbols)} symbols for CANSLIM setups")
    
    # Use a single fallback session if needed
    aio_session = None
    
    async def get_session():
        nonlocal aio_session
        if aio_session is None:
            aio_session = aiohttp.ClientSession()
        return aio_session
    
    try:
        # Assess market conditions first
        print("\n📈 Assessing market conditions...")
        spy_df = dfs.get("SPY")
        if spy_df is not None and len(spy_df) >= 50:
            market_assessment = assess_market_condition_from_df(spy_df)
        else:
            session = await get_session()
            market_assessment = await assess_market_condition(session)
        results["market_assessment"] = market_assessment
        
        print(f"Market Recommendation: {market_assessment.get('recommendation', 'unknown')}")
        print(f"Reason: {market_assessment.get('reason', 'N/A')}\n")
        
        # Scan for TQQQ opportunities
        tqqq_df = dfs.get("TQQQ")
        print("🔍 Checking TQQQ entry opportunities...")
        if tqqq_df is not None and len(tqqq_df) >= 100:
            results["tqqq_entry"] = scan_tqqq_entry_from_df(tqqq_df)
        else:
            session = await get_session()
            results["tqqq_entry"] = await scan_tqqq_entry(session)
        
        print("🔍 Checking TQQQ exit signals...")
        if tqqq_df is not None and len(tqqq_df) >= 100:
            results["tqqq_exit"] = scan_tqqq_exit_from_df(tqqq_df)
        else:
            session = await get_session()
            results["tqqq_exit"] = await scan_tqqq_exit(session)
        
        # Scan individual stocks for CANSLIM setups
        print(f"\n🔍 Scanning individual stocks for CANSLIM setups...")
        for symbol in symbols:
            try:
                df = dfs.get(symbol)
                if df is not None and len(df) >= 200:
                    signal = scan_canslim_stock_from_df(symbol, df)
                else:
                    session = await get_session()
                    signal = await scan_canslim_stock(symbol, session)
                
                if signal:
                    results["canslim_stocks"].append(signal)
                
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                continue
    finally:
        if aio_session:
            await aio_session.close()
    
    # Print summary
    print("\n" + "="*80)
    print("VIBIA J. HYBRID STRATEGY SUMMARY")
    print("="*80)
    print(f"Market Condition: {market_assessment.get('recommendation', 'unknown')}")
    print(f"CANSLIM Stock Setups: {len(results['canslim_stocks'])}")
    print(f"TQQQ Entry Signal: {'YES' if results['tqqq_entry'] else 'NO'}")
    print(f"TQQQ Exit Signal: {'YES' if results['tqqq_exit'] else 'NO'}")
    print("="*80 + "\n")
    
    return results


async def run_and_store_vibia_hybrid():
    """
    Run Vibia J.'s hybrid strategy and store results in the database.
    """
    session = SessionLocal()
    try:
        # Get S&P 500 tickers for individual stock screening
        tickers = await get_sp500_tickers()
        print(f"📈 Loaded {len(tickers)} S&P 500 tickers")
        
        # Run screening
        results = await run_vibia_hybrid_screen(tickers)
        
        # Store results in database
        now = datetime.datetime.now()
        today = now.date()
        time_now = now.time().replace(microsecond=0)
        
        total_stored = 0
        
        # Store CANSLIM stock signals
        for signal in results["canslim_stocks"]:
            entry = VibiaHybridData(
                strategy="canslim_stock",
                symbol=signal["symbol"],
                signal_type=signal["signal_type"],
                data_date=today,
                data_time=time_now,
                data_json=json.dumps(signal),
            )
            session.add(entry)
            total_stored += 1
        
        # Store TQQQ entry signal
        if results["tqqq_entry"]:
            entry = VibiaHybridData(
                strategy="tqqq_swing",
                symbol="TQQQ",
                signal_type="buy",
                data_date=today,
                data_time=time_now,
                data_json=json.dumps(results["tqqq_entry"]),
            )
            session.add(entry)
            total_stored += 1
        
        # Store TQQQ exit signal
        if results["tqqq_exit"]:
            entry = VibiaHybridData(
                strategy="tqqq_swing",
                symbol="TQQQ",
                signal_type="sell",
                data_date=today,
                data_time=time_now,
                data_json=json.dumps(results["tqqq_exit"]),
            )
            session.add(entry)
            total_stored += 1
        
        # Store market assessment
        if results["market_assessment"]:
            entry = VibiaHybridData(
                strategy="market_assessment",
                symbol="SPY",
                signal_type="assessment",
                data_date=today,
                data_time=time_now,
                data_json=json.dumps(results["market_assessment"]),
            )
            session.add(entry)
            total_stored += 1
        
        session.commit()
        print(f"✅ Stored {total_stored} signals in database for {today} {time_now}")
        
        return results
        
    except Exception as e:
        print(f"❌ Error in run_and_store_vibia_hybrid: {e}")
        session.rollback()
        raise
    finally:
        session.close()


# ==================== COMMAND LINE INTERFACE ====================

if __name__ == "__main__":
    """
    Entry point for running Vibia J.'s hybrid strategy as a script.
    
    Usage:
        python vibia_j_hybrid_strategy.py
    """
    asyncio.run(run_and_store_vibia_hybrid())
