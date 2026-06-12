"""
Marios Stamatoudis Swing Trading Strategy
------------------------------------------
Classic Breakouts, Episodic Pivots, and Parabolic Shorts

This module implements three distinct swing trading strategies developed by Marios Stamatoudis:

1. **Classic Breakouts:** Identifies stocks with significant moves (1-6 months, typically 30-100%)
   followed by an orderly 2-week to 2-month consolidation. Entry occurs when the price breaks
   above a trend line, with a stop loss at the breakout day's low. Profits are taken partially
   at 2.5-3x average daily range (ADR), and the rest is trailed using 10-day or 20-day moving
   averages, moving stops to break-even after initial gains.

2. **Episodic Pivots:** Focuses on "sleepy stocks" that gap up 5% or more due to a catalyst
   (e.g., earnings surprise, drug approval). These are often beaten-down names springing back
   to life. Entry varies, but a common objective point is the opening range high, with the
   stop loss at the day's low.

3. **Parabolic Shorts:** Targets stocks that have made rapid, substantial moves (100-400%),
   betting on mean reversion. Entry is triggered by the first sign of momentum loss, such as
   breaking the opening range low or failing at VWAP after an initial crack. The stop loss is
   placed at the day's high or the entry candle's high.

The strategy can be run as a standalone script (for scheduled jobs) or imported as a module.
Results are stored in the SwingTradeData table in the database for later retrieval via API.
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

import time

from aignitequant.app.db import SessionLocal, SwingTradeData
from aignitequant.app.services.polygon import get_polygon_data
from aignitequant.app.services.sp500 import get_sp500_tickers
from aignitequant.app.services.market_data import get_dataframe_from_db, get_multiple_dataframes_from_db


# ==================== CONFIGURATION ====================

# Classic Breakout Parameters
CLASSIC_MIN_PRIOR_MOVE_PCT = 0.30  # 30% minimum move before consolidation
CLASSIC_MAX_PRIOR_MOVE_PCT = 1.00  # 100% maximum move
CLASSIC_LOOKBACK_MONTHS = 6  # Look back 1-6 months for the move
CLASSIC_MIN_CONSOLIDATION_DAYS = 14  # 2 weeks minimum
CLASSIC_MAX_CONSOLIDATION_DAYS = 60  # 2 months maximum
CLASSIC_CONSOLIDATION_RANGE_PCT = 0.15  # 15% max range during consolidation
CLASSIC_ADR_MULTIPLIER = 2.75  # 2.5-3x ADR for first profit target

# Episodic Pivot Parameters
EPISODIC_MIN_GAP_PCT = 0.05  # 5% minimum gap up
EPISODIC_LOOKBACK_DAYS = 90  # Look for beaten-down names
EPISODIC_MIN_DECLINE_PCT = -0.20  # Stock should be down 20%+ before pivot
EPISODIC_OPENING_RANGE_MINUTES = 30  # First 30 minutes of trading

# Parabolic Short Parameters
PARABOLIC_MIN_MOVE_PCT = 1.00  # 100% minimum move
PARABOLIC_MAX_MOVE_PCT = 4.00  # 400% maximum move
PARABOLIC_LOOKBACK_DAYS = 90  # Look back 3 months
PARABOLIC_RSI_THRESHOLD = 70  # RSI above 70 indicates overbought


# ==================== HELPER FUNCTIONS ====================

def calculate_adr(df: pd.DataFrame, period: int = 20) -> float:
    """
    Calculate Average Daily Range (ADR).
    
    Args:
        df: DataFrame with 'high' and 'low' columns
        period: Number of days to calculate average
        
    Returns:
        Average daily range
    """
    if len(df) < period:
        return 0
    
    daily_ranges = df['high'] - df['low']
    return daily_ranges.tail(period).mean()


def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Relative Strength Index (RSI).
    
    Args:
        df: DataFrame with 'close' column
        period: RSI period
        
    Returns:
        Series with RSI values
    """
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calculate_vwap(df: pd.DataFrame) -> float:
    """
    Calculate Volume Weighted Average Price (VWAP) for the last day.
    
    Args:
        df: DataFrame with 'high', 'low', 'close', 'volume' columns
        
    Returns:
        VWAP value
    """
    if len(df) < 1:
        return 0
    
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    return (typical_price * df['volume']).sum() / df['volume'].sum()


def detect_trendline_breakout(df: pd.DataFrame, consolidation_start: int) -> Tuple[bool, float]:
    """
    Detect if price breaks above a trendline drawn from consolidation highs.
    
    Args:
        df: DataFrame with price data
        consolidation_start: Index where consolidation begins
        
    Returns:
        Tuple of (breakout_detected, breakout_level)
    """
    if len(df) < consolidation_start + 2:
        return False, 0
    
    # Get consolidation period
    consolidation_df = df.iloc[consolidation_start:]
    
    # Find resistance level (highest high during consolidation)
    resistance = consolidation_df['high'].max()
    
    # Check if last close is above resistance
    last_close = df['close'].iloc[-1]
    breakout = last_close > resistance
    
    return breakout, resistance


def detect_gap_up(df: pd.DataFrame, min_gap_pct: float = 0.05) -> Tuple[bool, float]:
    """
    Detect if today's open gaps up from yesterday's close.
    
    Args:
        df: DataFrame with 'open' and 'close' columns
        min_gap_pct: Minimum gap percentage
        
    Returns:
        Tuple of (gap_detected, gap_percentage)
    """
    if len(df) < 2:
        return False, 0
    
    prev_close = df['close'].iloc[-2]
    today_open = df['open'].iloc[-1]
    
    gap_pct = (today_open - prev_close) / prev_close
    
    return gap_pct >= min_gap_pct, gap_pct


def is_momentum_failing(df: pd.DataFrame, rsi_threshold: float = 70) -> bool:
    """
    Detect if momentum is failing on a parabolic move.

    NOTE — intraday vs daily limitation:
    Marios' Parabolic Short is fundamentally an intraday strategy. His entry trigger
    ("break of the opening range low" or "failed VWAP reclaim") requires intraday bars.
    On daily OHLCV data we can only approximate these signals:

    - "Breaking opening range low" → approximated as: close finishes in the LOWER THIRD
      of the day's high-low range. (close < open alone is not sufficient; close below
      the low is logically impossible and was a prior bug here.)
    - VWAP → approximated as a 20-day rolling VWAP (see calculate_vwap). Not the same as
      intraday VWAP. Use intraday_bars data if a tighter trigger is required.

    Args:
        df: DataFrame with daily OHLCV data.
        rsi_threshold: RSI level above which the stock is considered overbought.

    Returns:
        True if momentum appears to be failing on daily bars.
    """
    if len(df) < 20:
        return False

    # Calculate RSI
    rsi = calculate_rsi(df)
    current_rsi = rsi.iloc[-1]

    # RSI rolling over from overbought territory
    rsi_rolling_over = current_rsi < rsi.iloc[-2] and rsi.iloc[-2] > rsi_threshold

    # Lower highs over the last 5 days
    recent_highs = df['high'].tail(5)
    lower_highs = recent_highs.iloc[-1] < recent_highs.max()

    # Daily-bar proxy for "close breaks opening range low":
    # close finishes in the lower third of the day's range.
    # (close < low is always False on OHLCV bars and was previously a bug here.)
    day_range = df['high'].iloc[-1] - df['low'].iloc[-1]
    if day_range > 0:
        lower_third_boundary = df['low'].iloc[-1] + (day_range / 3)
        close_in_lower_third = df['close'].iloc[-1] <= lower_third_boundary
    else:
        close_in_lower_third = False

    return rsi_rolling_over or (lower_highs and close_in_lower_third)


# ==================== SYNC FROM-DF SCANNER FUNCTIONS ====================

def scan_classic_breakout_from_df(symbol: str, df: pd.DataFrame, spy_df: Optional[pd.DataFrame] = None) -> Optional[Dict]:
    """Sync version of scan_classic_breakout that works on a pre-loaded DataFrame.

    Args:
        symbol:  Ticker symbol.
        df:      Daily OHLCV DataFrame for the stock (lowercase columns, sorted ascending).
        spy_df:  Optional SPY daily OHLCV DataFrame for relative-strength check.
                 When provided, the stock must outperform SPY over the last 63 trading
                 days (≈3 months) — a key Marios Stamatoudis selection criterion.
    """
    try:
        if df is None or len(df) < 200:
            return None

        df = df.sort_index()
        df['sma_50'] = df['close'].rolling(window=50).mean()
        df['sma_200'] = df['close'].rolling(window=200).mean()

        lookback_days = CLASSIC_LOOKBACK_MONTHS * 30
        lookback_df = df.tail(lookback_days)

        low_idx = lookback_df['low'].idxmin()
        low_idx_position = lookback_df.index.get_loc(low_idx)

        after_low = lookback_df.iloc[low_idx_position:]
        if len(after_low) < CLASSIC_MIN_CONSOLIDATION_DAYS:
            return None

        max_high = after_low['high'].max()
        min_low = after_low['low'].iloc[0]
        move_pct = (max_high - min_low) / min_low

        if move_pct < CLASSIC_MIN_PRIOR_MOVE_PCT or move_pct > CLASSIC_MAX_PRIOR_MOVE_PCT:
            return None

        max_high_idx = after_low['high'].idxmax()
        max_high_position = after_low.index.get_loc(max_high_idx)
        consolidation_start = max_high_position
        consolidation_df = after_low.iloc[consolidation_start:]

        if len(consolidation_df) < CLASSIC_MIN_CONSOLIDATION_DAYS:
            return None
        if len(consolidation_df) > CLASSIC_MAX_CONSOLIDATION_DAYS:
            consolidation_df = consolidation_df.tail(CLASSIC_MAX_CONSOLIDATION_DAYS)

        cons_high = consolidation_df['high'].max()
        cons_low = consolidation_df['low'].min()
        cons_range_pct = (cons_high - cons_low) / cons_low

        if cons_range_pct > CLASSIC_CONSOLIDATION_RANGE_PCT:
            return None

        # Marios' key structural requirement: consolidation must form HIGHER LOWS.
        # Price following the 10/20/50-day MA upward, not just staying in a range.
        # We verify this via linear regression on the consolidation lows — positive
        # slope means lows are trending up (supply drying up, demand building).
        if len(consolidation_df) >= 4:
            cons_lows = consolidation_df['low'].values
            x = np.arange(len(cons_lows))
            lows_slope = np.polyfit(x, cons_lows, 1)[0]
            if lows_slope <= 0:
                return None  # Lower lows — not a valid Marios consolidation
        else:
            return None

        # Marios requires stocks that are outperforming the market index.
        # Check: 3-month (63-day) return of the stock must beat SPY.
        rs_vs_spy = None
        if spy_df is not None and len(spy_df) >= 63 and len(df) >= 63:
            stock_return_63 = (df['close'].iloc[-1] / df['close'].iloc[-63]) - 1
            spy_return_63 = (spy_df['close'].iloc[-1] / spy_df['close'].iloc[-63]) - 1
            rs_vs_spy = round(float(stock_return_63 - spy_return_63) * 100, 2)  # excess return %
            if stock_return_63 <= spy_return_63:
                return None  # Underperforming the market — skip

        breakout, resistance_level = detect_trendline_breakout(df, len(df) - len(consolidation_df))
        if not breakout:
            return None

        stop_loss = df['low'].iloc[-1]
        adr = calculate_adr(df)
        entry_price = df['close'].iloc[-1]

        # Reject if price has already run far past the breakout level — signal is stale.
        # A valid entry is near the breakout point, not after a 15%+ extension.
        BREAKOUT_EXTENSION_LIMIT = 0.15
        if resistance_level > 0 and entry_price > resistance_level * (1 + BREAKOUT_EXTENSION_LIMIT):
            return None

        first_target = entry_price + (adr * CLASSIC_ADR_MULTIPLIER)
        sma_10 = df['close'].tail(10).mean()
        sma_20 = df['close'].tail(20).mean()

        signal = {
            "symbol": symbol,
            "strategy": "classic_breakout",
            "timestamp": datetime.datetime.now().isoformat(),
            "entry_price": float(entry_price),
            "stop_loss": float(stop_loss),
            "first_target": float(first_target),
            "trailing_stop_10d": float(sma_10),
            "trailing_stop_20d": float(sma_20),
            "resistance_level": float(resistance_level),
            "prior_move_pct": float(move_pct * 100),
            "consolidation_days": int(len(consolidation_df)),
            "consolidation_range_pct": float(cons_range_pct * 100),
            "consolidation_lows_slope": round(float(lows_slope), 4),
            "rs_vs_spy_63d": rs_vs_spy,  # excess return vs SPY over 63 days (%); None if SPY unavailable
            "adr": float(adr),
            "risk_reward": float((first_target - entry_price) / (entry_price - stop_loss)) if entry_price > stop_loss else 0,
            "above_sma_50": bool(entry_price > df['sma_50'].iloc[-1]) if not pd.isna(df['sma_50'].iloc[-1]) else False,
            "above_sma_200": bool(entry_price > df['sma_200'].iloc[-1]) if not pd.isna(df['sma_200'].iloc[-1]) else False,
        }

        print(f"✅ CLASSIC BREAKOUT: {symbol} @ ${entry_price:.2f} | Move: {move_pct*100:.1f}% | RR: {signal['risk_reward']:.2f}")
        return signal

    except Exception as e:
        print(f"Error scanning {symbol} for classic breakout (from_df): {e}")
        return None


def scan_episodic_pivot_from_df(symbol: str, df: pd.DataFrame) -> Optional[Dict]:
    """Sync version of scan_episodic_pivot that works on a pre-loaded DataFrame."""
    try:
        if df is None or len(df) < EPISODIC_LOOKBACK_DAYS:
            return None

        df = df.sort_index()
        lookback_df = df.tail(EPISODIC_LOOKBACK_DAYS)
        high_before = lookback_df['high'].iloc[:60].max()
        low_recent = lookback_df['low'].iloc[60:].min()
        decline_pct = (low_recent - high_before) / high_before

        if decline_pct > EPISODIC_MIN_DECLINE_PCT:
            return None

        gap_detected, gap_pct = detect_gap_up(df, EPISODIC_MIN_GAP_PCT)
        if not gap_detected:
            return None

        # Marios requires unusual volume on the gap day — confirms institutional participation.
        # Require gap-day volume ≥ 1.5× the prior 20-day average.
        if len(df) >= 21 and df['volume'].iloc[-1] > 0:
            avg_vol_20 = df['volume'].iloc[-21:-1].mean()
            rel_vol = df['volume'].iloc[-1] / avg_vol_20 if avg_vol_20 > 0 else 0
            if rel_vol < 1.5:
                return None  # Volume not confirming the gap
        else:
            rel_vol = None

        opening_range_high = df['high'].iloc[-1]
        entry_price = opening_range_high
        stop_loss = df['low'].iloc[-1]
        prior_high = lookback_df['high'].max()
        potential_pct = (prior_high - entry_price) / entry_price

        signal = {
            "symbol": symbol,
            "strategy": "episodic_pivot",
            "timestamp": datetime.datetime.now().isoformat(),
            "entry_price": float(entry_price),
            "stop_loss": float(stop_loss),
            "prior_high": float(prior_high),
            "gap_pct": float(gap_pct * 100),
            "decline_before_gap": float(decline_pct * 100),
            "potential_to_prior_high_pct": float(potential_pct * 100),
            "risk_reward": float((prior_high - entry_price) / (entry_price - stop_loss)) if entry_price > stop_loss else 0,
            "opening_range_high": float(opening_range_high),
            # gap_detected: True means a ≥5% gap-up was observed on daily bars.
            # Marios' episodic pivot requires a FUNDAMENTAL CATALYST (earnings surprise,
            # FDA approval, major contract, etc.) — this flag does NOT confirm that.
            # Always verify the catalyst manually before trading this signal.
            "gap_detected": True,
            "catalyst_note": "Gap-up ≥5% detected on daily bars — manual confirmation of fundamental catalyst required",
            "relative_volume": round(float(rel_vol), 2) if rel_vol is not None else None,
        }

        print(f"✅ EPISODIC PIVOT: {symbol} @ ${entry_price:.2f} | Gap: {gap_pct*100:.1f}% | RelVol: {rel_vol:.1f}x | Potential: {potential_pct*100:.1f}%")
        return signal

    except Exception as e:
        print(f"Error scanning {symbol} for episodic pivot (from_df): {e}")
        return None


def scan_parabolic_short_from_df(symbol: str, df: pd.DataFrame) -> Optional[Dict]:
    """Sync version of scan_parabolic_short that works on a pre-loaded DataFrame.

    ⚠️  ARCHITECTURAL LIMITATION — DAILY DATA ONLY:
    Marios Stamatoudis' Parabolic Short is an INTRADAY strategy. His precise entry
    triggers (break of the 30-minute opening range low, failed VWAP reclaim, intraday
    reversal candles) cannot be faithfully replicated from daily OHLCV bars alone.

    What this function provides is a daily-bar SCREENING LAYER that identifies stocks
    with the right macro conditions (100-400% parabolic move, overbought RSI, momentum
    rollover). Any signal from this function should be treated as a WATCHLIST CANDIDATE,
    not a ready-to-trade entry. The actual entry trigger must be confirmed using intraday
    data (see IntradayBar table / intraday_bars endpoint) before placing a short position.
    """
    try:
        if df is None or len(df) < PARABOLIC_LOOKBACK_DAYS:
            return None

        df = df.sort_index()
        lookback_df = df.tail(PARABOLIC_LOOKBACK_DAYS)
        low_point = lookback_df['low'].min()
        high_point = lookback_df['high'].max()
        move_pct = (high_point - low_point) / low_point

        if move_pct < PARABOLIC_MIN_MOVE_PCT or move_pct > PARABOLIC_MAX_MOVE_PCT:
            return None

        if not is_momentum_failing(df, PARABOLIC_RSI_THRESHOLD):
            return None

        rsi = calculate_rsi(df)
        current_rsi = rsi.iloc[-1]
        # NOTE: calculate_vwap on daily bars computes a 20-day rolling VWAP proxy.
        # This is NOT intraday VWAP. Marios uses intraday VWAP as a key entry/exit
        # reference; this value is provided for context only.
        vwap = calculate_vwap(df.tail(20))
        entry_price = df['close'].iloc[-1]
        stop_loss = df['high'].iloc[-1]
        sma_20 = df['close'].tail(20).mean()
        target_price = sma_20

        if target_price >= entry_price:
            target_price = entry_price * 0.85

        signal = {
            "symbol": symbol,
            "strategy": "parabolic_short",
            "timestamp": datetime.datetime.now().isoformat(),
            "entry_price": float(entry_price),
            "stop_loss": float(stop_loss),
            "target_price": float(target_price),
            "parabolic_move_pct": float(move_pct * 100),
            "rsi": float(current_rsi),
            "vwap": float(vwap),
            "sma_20": float(sma_20),
            "distance_from_high": float((high_point - entry_price) / high_point * 100),
            "risk_reward": float((entry_price - target_price) / (stop_loss - entry_price)) if stop_loss > entry_price else 0,
            "momentum_failing": True,
            "short_signal": True,
        }

        print(f"✅ PARABOLIC SHORT: {symbol} @ ${entry_price:.2f} | Move: {move_pct*100:.1f}% | RSI: {current_rsi:.1f}")
        return signal

    except Exception as e:
        print(f"Error scanning {symbol} for parabolic short (from_df): {e}")
        return None


# ==================== STRATEGY 1: CLASSIC BREAKOUT ====================

async def scan_classic_breakout(symbol: str, session: aiohttp.ClientSession) -> Optional[Dict]:
    """
    Scan for Classic Breakout pattern.
    
    Args:
        symbol: Stock symbol to scan
        session: aiohttp session
        
    Returns:
        Dict with signal details if pattern detected, None otherwise
    """
    try:
        df = await get_polygon_data(symbol, session)
        if df is None or len(df) < 200:
            return None
        
        # Ensure data is sorted by date
        df = df.sort_index()
        
        # Calculate moving averages for context
        df['sma_50'] = df['close'].rolling(window=50).mean()
        df['sma_200'] = df['close'].rolling(window=200).mean()
        
        # Find the prior significant move (30-100% in 1-6 months)
        lookback_days = CLASSIC_LOOKBACK_MONTHS * 30
        lookback_df = df.tail(lookback_days)
        
        # Find the low point before the move
        low_idx = lookback_df['low'].idxmin()
        low_idx_position = lookback_df.index.get_loc(low_idx)
        
        # Check if there was a significant move after the low
        after_low = lookback_df.iloc[low_idx_position:]
        if len(after_low) < CLASSIC_MIN_CONSOLIDATION_DAYS:
            return None
        
        max_high = after_low['high'].max()
        min_low = after_low['low'].iloc[0]
        move_pct = (max_high - min_low) / min_low
        
        if move_pct < CLASSIC_MIN_PRIOR_MOVE_PCT or move_pct > CLASSIC_MAX_PRIOR_MOVE_PCT:
            return None
        
        # Find consolidation period (2 weeks to 2 months of sideways action)
        # Look for period where price stays within a tight range
        max_high_idx = after_low['high'].idxmax()
        max_high_position = after_low.index.get_loc(max_high_idx)
        
        consolidation_start = max_high_position
        consolidation_df = after_low.iloc[consolidation_start:]
        
        if len(consolidation_df) < CLASSIC_MIN_CONSOLIDATION_DAYS:
            return None
        
        if len(consolidation_df) > CLASSIC_MAX_CONSOLIDATION_DAYS:
            consolidation_df = consolidation_df.tail(CLASSIC_MAX_CONSOLIDATION_DAYS)
        
        # Check if consolidation is tight (within 15% range)
        cons_high = consolidation_df['high'].max()
        cons_low = consolidation_df['low'].min()
        cons_range_pct = (cons_high - cons_low) / cons_low
        
        if cons_range_pct > CLASSIC_CONSOLIDATION_RANGE_PCT:
            return None

        # Marios' structural requirement: consolidation must form HIGHER LOWS.
        if len(consolidation_df) >= 4:
            cons_lows = consolidation_df['low'].values
            x = np.arange(len(cons_lows))
            lows_slope = np.polyfit(x, cons_lows, 1)[0]
            if lows_slope <= 0:
                return None  # Lower lows — not a valid Marios consolidation
        else:
            return None

        # Detect trendline breakout
        breakout, resistance_level = detect_trendline_breakout(df, len(df) - len(consolidation_df))

        if not breakout:
            return None

        # Calculate stop loss (breakout day's low)
        stop_loss = df['low'].iloc[-1]

        # Calculate first profit target (2.5-3x ADR)
        adr = calculate_adr(df)
        entry_price = df['close'].iloc[-1]

        # Reject if price has already run far past the breakout level — signal is stale.
        BREAKOUT_EXTENSION_LIMIT = 0.15
        if resistance_level > 0 and entry_price > resistance_level * (1 + BREAKOUT_EXTENSION_LIMIT):
            return None

        first_target = entry_price + (adr * CLASSIC_ADR_MULTIPLIER)

        # Calculate trailing stop levels
        sma_10 = df['close'].tail(10).mean()
        sma_20 = df['close'].tail(20).mean()

        signal = {
            "symbol": symbol,
            "strategy": "classic_breakout",
            "timestamp": datetime.datetime.now().isoformat(),
            "entry_price": float(entry_price),
            "stop_loss": float(stop_loss),
            "first_target": float(first_target),
            "trailing_stop_10d": float(sma_10),
            "trailing_stop_20d": float(sma_20),
            "resistance_level": float(resistance_level),
            "prior_move_pct": float(move_pct * 100),
            "consolidation_days": int(len(consolidation_df)),
            "consolidation_range_pct": float(cons_range_pct * 100),
            "consolidation_lows_slope": round(float(lows_slope), 4),
            "adr": float(adr),
            "risk_reward": float((first_target - entry_price) / (entry_price - stop_loss)) if entry_price > stop_loss else 0,
            "above_sma_50": bool(entry_price > df['sma_50'].iloc[-1]) if not pd.isna(df['sma_50'].iloc[-1]) else False,
            "above_sma_200": bool(entry_price > df['sma_200'].iloc[-1]) if not pd.isna(df['sma_200'].iloc[-1]) else False,
        }

        print(f"✅ CLASSIC BREAKOUT: {symbol} @ ${entry_price:.2f} | Move: {move_pct*100:.1f}% | RR: {signal['risk_reward']:.2f}")
        return signal

    except Exception as e:
        print(f"Error scanning {symbol} for classic breakout: {e}")
        return None


# ==================== STRATEGY 2: EPISODIC PIVOT ====================

async def scan_episodic_pivot(symbol: str, session: aiohttp.ClientSession) -> Optional[Dict]:
    """
    Scan for Episodic Pivot pattern (sleepy stocks gapping up on catalyst).
    
    Args:
        symbol: Stock symbol to scan
        session: aiohttp session
        
    Returns:
        Dict with signal details if pattern detected, None otherwise
    """
    try:
        df = await get_polygon_data(symbol, session)
        if df is None or len(df) < EPISODIC_LOOKBACK_DAYS:
            return None
        
        # Ensure data is sorted by date
        df = df.sort_index()
        
        # Check if stock was beaten down (down 20%+ in last 90 days)
        lookback_df = df.tail(EPISODIC_LOOKBACK_DAYS)
        high_before = lookback_df['high'].iloc[:60].max()  # High in first 60 days
        low_recent = lookback_df['low'].iloc[60:].min()     # Low in last 30 days
        
        decline_pct = (low_recent - high_before) / high_before
        
        if decline_pct > EPISODIC_MIN_DECLINE_PCT:
            return None  # Not beaten down enough
        
        # Detect gap up 5%+ today
        gap_detected, gap_pct = detect_gap_up(df, EPISODIC_MIN_GAP_PCT)

        if not gap_detected:
            return None

        # Require unusual volume on the gap day (≥1.5× 20-day average)
        if len(df) >= 21 and df['volume'].iloc[-1] > 0:
            avg_vol_20 = df['volume'].iloc[-21:-1].mean()
            rel_vol = df['volume'].iloc[-1] / avg_vol_20 if avg_vol_20 > 0 else 0
            if rel_vol < 1.5:
                return None
        else:
            rel_vol = None

        # Calculate entry point (opening range high)
        # Since we don't have intraday data, use today's high as proxy
        opening_range_high = df['high'].iloc[-1]
        entry_price = opening_range_high

        # Stop loss at day's low
        stop_loss = df['low'].iloc[-1]

        # Calculate potential (distance to prior high)
        prior_high = lookback_df['high'].max()
        potential_pct = (prior_high - entry_price) / entry_price

        signal = {
            "symbol": symbol,
            "strategy": "episodic_pivot",
            "timestamp": datetime.datetime.now().isoformat(),
            "entry_price": float(entry_price),
            "stop_loss": float(stop_loss),
            "prior_high": float(prior_high),
            "gap_pct": float(gap_pct * 100),
            "decline_before_gap": float(decline_pct * 100),
            "potential_to_prior_high_pct": float(potential_pct * 100),
            "risk_reward": float((prior_high - entry_price) / (entry_price - stop_loss)) if entry_price > stop_loss else 0,
            "opening_range_high": float(opening_range_high),
            "gap_detected": True,
            "catalyst_note": "Gap-up ≥5% detected on daily bars — manual confirmation of fundamental catalyst required",
            "relative_volume": round(float(rel_vol), 2) if rel_vol is not None else None,
        }

        print(f"✅ EPISODIC PIVOT: {symbol} @ ${entry_price:.2f} | Gap: {gap_pct*100:.1f}% | RelVol: {rel_vol:.1f}x | Potential: {potential_pct*100:.1f}%")
        return signal

    except Exception as e:
        print(f"Error scanning {symbol} for episodic pivot: {e}")
        return None


# ==================== STRATEGY 3: PARABOLIC SHORT ====================

async def scan_parabolic_short(symbol: str, session: aiohttp.ClientSession) -> Optional[Dict]:
    """
    Scan for Parabolic Short pattern (extreme moves ready to reverse).

    ⚠️  ARCHITECTURAL LIMITATION — DAILY DATA ONLY:
    Marios' Parabolic Short requires intraday entry triggers. Signals from this function
    are daily-bar watchlist candidates only — confirm with intraday data before trading.

    Args:
        symbol: Stock symbol to scan
        session: aiohttp session

    Returns:
        Dict with signal details if pattern detected, None otherwise
    """
    try:
        df = await get_polygon_data(symbol, session)
        if df is None or len(df) < PARABOLIC_LOOKBACK_DAYS:
            return None
        
        # Ensure data is sorted by date
        df = df.sort_index()
        
        # Check for parabolic move (100-400% in last 90 days)
        lookback_df = df.tail(PARABOLIC_LOOKBACK_DAYS)
        low_point = lookback_df['low'].min()
        high_point = lookback_df['high'].max()
        
        move_pct = (high_point - low_point) / low_point
        
        if move_pct < PARABOLIC_MIN_MOVE_PCT or move_pct > PARABOLIC_MAX_MOVE_PCT:
            return None
        
        # Check if momentum is failing
        if not is_momentum_failing(df, PARABOLIC_RSI_THRESHOLD):
            return None
        
        # Calculate RSI
        rsi = calculate_rsi(df)
        current_rsi = rsi.iloc[-1]
        
        # 20-day rolling VWAP proxy (NOT intraday VWAP — for context only)
        vwap = calculate_vwap(df.tail(20))

        # Entry price (current close)
        entry_price = df['close'].iloc[-1]
        
        # Stop loss at today's high or entry candle's high
        stop_loss = df['high'].iloc[-1]
        
        # Target based on mean reversion (move back to 20-day SMA)
        sma_20 = df['close'].tail(20).mean()
        target_price = sma_20
        
        # Make sure target is below entry (for short)
        if target_price >= entry_price:
            target_price = entry_price * 0.85  # Default 15% target
        
        signal = {
            "symbol": symbol,
            "strategy": "parabolic_short",
            "timestamp": datetime.datetime.now().isoformat(),
            "entry_price": float(entry_price),
            "stop_loss": float(stop_loss),
            "target_price": float(target_price),
            "parabolic_move_pct": float(move_pct * 100),
            "rsi": float(current_rsi),
            "vwap": float(vwap),
            "sma_20": float(sma_20),
            "distance_from_high": float((high_point - entry_price) / high_point * 100),
            "risk_reward": float((entry_price - target_price) / (stop_loss - entry_price)) if stop_loss > entry_price else 0,
            "momentum_failing": True,
            "short_signal": True,  # This is a short position
        }
        
        print(f"✅ PARABOLIC SHORT: {symbol} @ ${entry_price:.2f} | Move: {move_pct*100:.1f}% | RSI: {current_rsi:.1f}")
        return signal
        
    except Exception as e:
        print(f"Error scanning {symbol} for parabolic short: {e}")
        return None


# ==================== MAIN SCREENING FUNCTIONS ====================

async def run_swing_trade_screen(symbols: List[str], strategies: List[str] = None) -> Dict[str, List[Dict]]:
    """
    Run swing trade strategies on a list of symbols.

    Args:
        symbols: List of stock symbols to scan
        strategies: List of strategies to run. Options: ['classic_breakout', 'episodic_pivot', 'parabolic_short']
                   If None, runs all strategies.

    Returns:
        Dict with strategy names as keys and lists of signals as values
    """
    if strategies is None:
        strategies = ['classic_breakout', 'episodic_pivot', 'parabolic_short']

    results = {strategy: [] for strategy in strategies}

    # --- Phase 1: Batch read from DB ---
    print(f"🔍 Scanning {len(symbols)} symbols for swing trade opportunities...")
    print(f"📊 Active strategies: {', '.join(strategies)}")

    t0 = time.time()
    all_dfs = get_multiple_dataframes_from_db(symbols + ["SPY"], days=730)
    db_time = time.time() - t0
    print(f"📂 DB batch read: {len(all_dfs)} DataFrames in {db_time:.2f}s")

    # SPY needed for Classic Breakout relative-strength check
    spy_df = all_dfs.get("SPY")

    api_fallback_symbols = []

    for symbol in symbols:
        df = all_dfs.get(symbol)
        if df is not None and len(df) >= 90:
            try:
                if 'classic_breakout' in strategies:
                    signal = scan_classic_breakout_from_df(symbol, df, spy_df=spy_df)
                    if signal:
                        results['classic_breakout'].append(signal)

                if 'episodic_pivot' in strategies:
                    signal = scan_episodic_pivot_from_df(symbol, df)
                    if signal:
                        results['episodic_pivot'].append(signal)

                if 'parabolic_short' in strategies:
                    signal = scan_parabolic_short_from_df(symbol, df)
                    if signal:
                        results['parabolic_short'].append(signal)
            except Exception as e:
                print(f"Error processing {symbol} from DB: {e}")
        else:
            api_fallback_symbols.append(symbol)

    # --- Phase 2: API fallback for missing symbols ---
    if api_fallback_symbols:
        print(f"🌐 API fallback for {len(api_fallback_symbols)} symbols...")
        async with aiohttp.ClientSession() as session:
            for symbol in api_fallback_symbols:
                try:
                    if 'classic_breakout' in strategies:
                        signal = await scan_classic_breakout(symbol, session)
                        if signal:
                            results['classic_breakout'].append(signal)

                    if 'episodic_pivot' in strategies:
                        signal = await scan_episodic_pivot(symbol, session)
                        if signal:
                            results['episodic_pivot'].append(signal)

                    if 'parabolic_short' in strategies:
                        signal = await scan_parabolic_short(symbol, session)
                        if signal:
                            results['parabolic_short'].append(signal)

                    await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"Error processing {symbol}: {e}")
                    continue

    # Print summary
    print("\n" + "="*60)
    print("SWING TRADE SCAN SUMMARY")
    print("="*60)
    for strategy, signals in results.items():
        print(f"{strategy.upper()}: {len(signals)} signals")
    print("="*60 + "\n")

    return results


async def run_and_store_swing_trades(strategies: List[str] = None):
    """
    Run swing trade strategies on S&P 500 tickers and store results in the database.

    Args:
        strategies: List of strategies to run. If None, runs all strategies.
    """
    session = SessionLocal()
    try:
        overall_start = time.time()
        # Get S&P 500 tickers
        tickers = await get_sp500_tickers()
        print(f"📈 Loaded {len(tickers)} S&P 500 tickers")

        # Run screening
        results = await run_swing_trade_screen(tickers, strategies)

        # Store results in database
        now = datetime.datetime.now()
        today = now.date()
        time_now = now.time().replace(microsecond=0)

        total_stored = 0
        for strategy, signals in results.items():
            for signal in signals:
                entry = SwingTradeData(
                    strategy=strategy,
                    symbol=signal.get("symbol"),
                    data_date=today,
                    data_time=time_now,
                    data_json=json.dumps(signal),
                )
                session.add(entry)
                total_stored += 1

        session.commit()
        print(f"✅ Stored {total_stored} swing trade signals in database for {today} {time_now}")
        elapsed = time.time() - overall_start
        print(f"⏱️ Swing trade screen total time: {elapsed:.2f}s")

        return results

    except Exception as e:
        print(f"❌ Error in run_and_store_swing_trades: {e}")
        session.rollback()
        raise
    finally:
        session.close()


# ==================== COMMAND LINE INTERFACE ====================

if __name__ == "__main__":
    """
    Entry point for running the swing trade strategies as a script.

    Usage:
        python swing_trade_strategy.py                    # Run all strategies on S&P 500
        python swing_trade_strategy.py classic_breakout   # Run only classic breakout
        python swing_trade_strategy.py episodic_pivot     # Run only episodic pivot
        python swing_trade_strategy.py parabolic_short    # Run only parabolic short
    """
    import sys

    strategies_to_run = None
    if len(sys.argv) > 1:
        strategy_arg = sys.argv[1].lower()
        if strategy_arg in ['classic_breakout', 'episodic_pivot', 'parabolic_short']:
            strategies_to_run = [strategy_arg]
        else:
            print(f"Unknown strategy: {strategy_arg}")
            print("Available strategies: classic_breakout, episodic_pivot, parabolic_short")
            sys.exit(1)

    asyncio.run(run_and_store_swing_trades(strategies_to_run))
      else:
            print(f"Unknown strategy: {strategy_arg}")
            print("Available strategies: classic_breakout, episodic_pivot, parabolic_short")
            sys.exit(1)
    
    asyncio.run(run_and_store_swing_trades(strategies_to_run))
