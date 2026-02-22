"""
Felix Strategy – Institutional 50-SMA Breakout Scanner
------------------------------------------------------
Scans S&P 500 stocks for institutional-quality buying signals by detecting:

1. **Stock was trading BELOW the 50-day SMA for a sustained period** – at least
   10 of the prior 15 trading days must have closed below the 50-SMA.  This
   eliminates stocks that are already riding above the MA (continuation, not
   a fresh breakout) and focuses only on true reclaims.

2. **Price then crossed above the 50-day SMA** in the last 3 trading days.

3. **The 50-SMA is curving upward** (positive acceleration – the slope itself
   is increasing), confirming the trend is strengthening.

4. **Volume spike on the crossover day** materially above the 50-day average
   volume, indicating big (institutional) buyers are stepping in – not weak
   retail order flow.

Why this works:
- You want to catch the moment institutions start accumulating a beaten-down
  stock.  Price living below the 50-SMA for weeks then suddenly reclaiming
  it on heavy volume is the classic footprint of block buying.
- Stocks already above the MA are filtered out – those are continuation
  moves, not the high-conviction inflection points this strategy targets.

Results are stored in the FelixData table in the database.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import json
import asyncio
import aiohttp
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from aignitequant.app.db import SessionLocal, FelixData
from aignitequant.app.services.polygon import get_polygon_data
from aignitequant.app.services.sp500 import get_sp500_tickers
from aignitequant.app.services.market_data import get_dataframe_from_db, get_multiple_dataframes_from_db


# ==================== CONFIGURATION ====================

# 50-SMA crossover look-back (trading days)
CROSSOVER_LOOKBACK_DAYS = 5

# Moving-average period
SMA_PERIOD = 50

# Window used to measure the 50-SMA curvature (slope + acceleration)
SMA_SLOPE_WINDOW = 10          # days used to measure slope
SMA_SLOPE_ACCELERATION_WINDOW = 5  # days to measure acceleration (slope of slope)

# Volume thresholds – crossover-day volume relative to 50-day average.
VOLUME_SPIKE_MULTIPLIER = 1.3   # 1.3× average volume minimum ("above average")
VOLUME_SPIKE_IDEAL = 2.0        # 2.0× is ideal (strong institutional footprint)

# Relative-volume ranking threshold (how the crossover day ranks vs. last 50 days).
VOLUME_PERCENTILE_MIN = 0.65    # top-35 percentile

# --- "Below the 50-SMA for a while" filter ---
# Before the crossover, the stock must have traded BELOW the 50-SMA for at
# least this many of the prior N days.  This eliminates stocks that were
# already riding above the MA and just had a brief 1-day dip below it.
BELOW_SMA_LOOKBACK = 20         # look at this many days before the cross day
BELOW_SMA_MIN_DAYS = 10         # at least this many of those days must be below

# Also reject if, right before the cross, price was ABOVE the 50-SMA for
# this many consecutive days (catches stocks that have been above for weeks
# and are NOT fresh breakouts).
ABOVE_SMA_CONSEC_REJECT = 5     # if 5+ consecutive days above SMA before cross → reject


# ==================== CORE DETECTION ====================

def detect_felix_signal(df: pd.DataFrame, ticker: str = "") -> Dict:
    """
    Detect whether *ticker* shows a Felix Strategy buy signal.

    Parameters
    ----------
    df : pd.DataFrame
        OHLCV data with columns [open, high, low, close, volume] and a
        datetime index, sorted ascending.
    ticker : str
        Ticker symbol (for logging).

    Returns
    -------
    dict
        {"detected": True/False, ...details...}
    """
    if df is None or len(df) < SMA_PERIOD + SMA_SLOPE_WINDOW + 5:
        return {"detected": False, "reason": "insufficient_data"}

    df = df.copy()

    # --- 1. Compute the 50-day SMA ---
    df["SMA50"] = df["close"].rolling(window=SMA_PERIOD).mean()
    df["AvgVol50"] = df["volume"].rolling(window=SMA_PERIOD).mean()

    # Drop NaN rows from rolling calcs
    df = df.dropna(subset=["SMA50", "AvgVol50"])
    if len(df) < CROSSOVER_LOOKBACK_DAYS + SMA_SLOPE_WINDOW:
        return {"detected": False, "reason": "insufficient_data_after_sma"}

    # --- 2. Detect price crossing UP through the 50-SMA in the last N days ---
    # Mark every day as below / above the SMA
    df["below_sma"] = df["close"] < df["SMA50"]
    df["above_sma"] = df["close"] > df["SMA50"]

    df["prev_close"] = df["close"].shift(1)
    df["prev_sma50"] = df["SMA50"].shift(1)

    # Cross-up: previous close was below prev SMA50, current close is above current SMA50
    df["cross_up"] = (df["prev_close"] < df["prev_sma50"]) & (df["close"] > df["SMA50"])

    recent = df.tail(CROSSOVER_LOOKBACK_DAYS)
    cross_days = recent[recent["cross_up"]]

    if cross_days.empty:
        return {"detected": False, "reason": "no_crossover"}

    # Take the most recent crossover day
    cross_row = cross_days.iloc[-1]
    cross_date = cross_row.name
    cross_idx = df.index.get_loc(cross_date)

    # --- 2b. FILTER: stock must have been BELOW the 50-SMA for a while ---
    # Look at the BELOW_SMA_LOOKBACK days *before* the cross day
    pre_cross_start = max(0, cross_idx - BELOW_SMA_LOOKBACK)
    pre_cross_segment = df.iloc[pre_cross_start:cross_idx]  # excludes cross day itself

    if len(pre_cross_segment) < BELOW_SMA_LOOKBACK:
        # Not enough history – still allow if most days were below
        days_below = int(pre_cross_segment["below_sma"].sum())
        required = max(3, int(len(pre_cross_segment) * 0.65))
    else:
        days_below = int(pre_cross_segment["below_sma"].sum())
        required = BELOW_SMA_MIN_DAYS

    if days_below < required:
        return {
            "detected": False,
            "reason": "price_already_above_sma",
            "days_below_sma": days_below,
            "required": required,
            "lookback": len(pre_cross_segment),
        }

    # --- 2c. FILTER: reject if price was consecutively above SMA right before cross ---
    # This catches stocks that have been riding above the MA and just briefly
    # touched it before bouncing – we want TRUE reclaims, not continuation dips.
    if len(pre_cross_segment) >= ABOVE_SMA_CONSEC_REJECT:
        tail_above = pre_cross_segment["above_sma"].iloc[-ABOVE_SMA_CONSEC_REJECT:]
        if tail_above.all():
            return {
                "detected": False,
                "reason": "already_above_sma_recently",
                "consecutive_days_above": ABOVE_SMA_CONSEC_REJECT,
            }

    # --- 3. Check 50-SMA is curving upward (positive slope + positive acceleration) ---
    sma_slope_ok, slope_details = _check_sma_curving_up(df, cross_idx)
    if not sma_slope_ok:
        return {"detected": False, "reason": "sma_not_curving_up", "slope_details": slope_details}

    # --- 4. Volume spike on the crossover day ---
    vol_ok, vol_details = _check_volume_spike(df, cross_row)
    if not vol_ok:
        return {"detected": False, "reason": "no_volume_spike", "volume_details": vol_details}

    # --- Build result ---
    current = df.iloc[-1]
    return {
        "detected": True,
        "ticker": ticker,
        "cross_date": cross_date.strftime("%Y-%m-%d"),
        "cross_price": round(float(cross_row["close"]), 2),
        "current_price": round(float(current["close"]), 2),
        "sma50": round(float(cross_row["SMA50"]), 2),
        "sma50_current": round(float(current["SMA50"]), 2),
        "sma50_slope": round(slope_details["slope"], 4),
        "sma50_acceleration": round(slope_details["acceleration"], 6),
        "days_below_sma_before_cross": days_below,
        "below_sma_lookback": len(pre_cross_segment),
        "cross_day_volume": int(cross_row["volume"]),
        "avg_volume_50d": int(cross_row["AvgVol50"]),
        "volume_ratio": round(float(cross_row["volume"] / cross_row["AvgVol50"]), 2),
        "volume_percentile": round(vol_details["percentile"], 2),
        "institutional_strength": _rate_institutional_strength(
            cross_row["volume"] / cross_row["AvgVol50"],
            slope_details["slope"],
            slope_details["acceleration"],
        ),
        "signal_quality": _rate_signal_quality(
            cross_row["volume"] / cross_row["AvgVol50"],
            slope_details["slope"],
            slope_details["acceleration"],
        ),
    }


# ==================== HELPER FUNCTIONS ====================

def _check_sma_curving_up(df: pd.DataFrame, cross_idx: int) -> Tuple[bool, Dict]:
    """
    Verify the 50-SMA is *curving upward* at the crossover point.

    KEY INSIGHT: When a stock has been below the 50-MA for a while, the MA
    is typically still declining when price first crosses above it.  That is
    normal.  What we want is **positive acceleration** – the slope is
    becoming *less negative* (or turning positive), meaning the MA is
    bending upward.  We do NOT require the absolute slope to be positive,
    only that the trend is changing direction.

    We measure:
    • slope  = linear-regression slope of the SMA50 over the last N days.
    • acceleration = slope_second_half − slope_first_half.  Positive means
      the MA is curving up (concave-up).
    """
    start = max(0, cross_idx - SMA_SLOPE_WINDOW)
    sma_segment = df["SMA50"].iloc[start:cross_idx + 1].values

    if len(sma_segment) < 5:
        return False, {"slope": 0, "acceleration": 0, "reason": "too_short"}

    # Slope via linear regression
    x = np.arange(len(sma_segment))
    slope = np.polyfit(x, sma_segment, 1)[0]

    # Acceleration = slope of the last half minus slope of first half
    mid = len(sma_segment) // 2
    if mid < 2:
        accel = 0.0
    else:
        first_half = sma_segment[:mid]
        second_half = sma_segment[mid:]
        slope_first = np.polyfit(np.arange(len(first_half)), first_half, 1)[0]
        slope_second = np.polyfit(np.arange(len(second_half)), second_half, 1)[0]
        accel = slope_second - slope_first

    details = {
        "slope": float(slope),
        "acceleration": float(accel),
        "slope_first_half": float(slope_first) if mid >= 2 else 0.0,
        "slope_second_half": float(slope_second) if mid >= 2 else 0.0,
    }

    # --- CURVING-UP CHECK ---
    # The MA is "curving up" when the second-half slope is greater than the
    # first-half slope (positive acceleration).  The overall slope can still
    # be negative – that just means the MA hasn't fully turned yet, but it
    # IS bending upward.
    #
    # We also accept cases where the recent slope (second half) is already
    # positive, even if acceleration is tiny – the MA has already turned.
    recent_slope = details.get("slope_second_half", slope)

    if accel > 0:
        # MA is curving up – this is the primary signal.  Pass.
        return True, details

    if recent_slope > 0 and slope > -0.1:
        # Recent slope is positive and overall slope is near-flat or positive.
        # The MA has already started rising.  Pass.
        return True, details

    # Neither condition met – the MA is still falling and accelerating down
    # or flat/decelerating.  Reject.
    if accel <= 0:
        details["reason"] = "ma_not_curving_up"
    else:
        details["reason"] = "slope_too_negative"
    return False, details


def _check_volume_spike(df: pd.DataFrame, cross_row: pd.Series) -> Tuple[bool, Dict]:
    """
    Check that the crossover day's volume shows institutional-level demand.
    """
    vol = cross_row["volume"]
    avg_vol = cross_row["AvgVol50"]
    ratio = vol / avg_vol if avg_vol > 0 else 0

    # Percentile rank within last 50 days
    cross_idx = df.index.get_loc(cross_row.name)
    lookback_start = max(0, cross_idx - SMA_PERIOD)
    recent_vols = df["volume"].iloc[lookback_start:cross_idx + 1]
    percentile = (recent_vols < vol).sum() / len(recent_vols) if len(recent_vols) > 0 else 0

    details = {
        "volume": int(vol),
        "avg_volume": int(avg_vol),
        "ratio": round(float(ratio), 2),
        "percentile": round(float(percentile), 4),
    }

    if ratio < VOLUME_SPIKE_MULTIPLIER:
        details["reason"] = f"ratio {ratio:.2f}x < {VOLUME_SPIKE_MULTIPLIER}x threshold"
        return False, details

    if percentile < VOLUME_PERCENTILE_MIN:
        details["reason"] = f"percentile {percentile:.0%} < {VOLUME_PERCENTILE_MIN:.0%} threshold"
        return False, details

    return True, details


def _rate_institutional_strength(vol_ratio: float, slope: float, accel: float) -> str:
    """
    Qualitative rating of how strong the institutional buying signal is.
    """
    score = 0
    if vol_ratio >= VOLUME_SPIKE_IDEAL:
        score += 3
    elif vol_ratio >= 1.5:
        score += 2
    elif vol_ratio >= VOLUME_SPIKE_MULTIPLIER:
        score += 1

    if slope > 0.2:
        score += 2
    elif slope > 0:
        score += 1
    # Negative slope is OK if acceleration compensates (handled below)

    if accel > 0.1:
        score += 3
    elif accel > 0.03:
        score += 2
    elif accel > 0:
        score += 1

    if score >= 6:
        return "VERY STRONG"
    elif score >= 4:
        return "STRONG"
    elif score >= 3:
        return "MODERATE"
    else:
        return "WEAK"


def _rate_signal_quality(vol_ratio: float, slope: float, accel: float) -> int:
    """
    Numeric signal quality score (0-100).
    """
    # Volume component (0-40)
    vol_score = min(40, (vol_ratio / VOLUME_SPIKE_IDEAL) * 40)

    # Slope component (0-30)
    slope_score = min(30, max(0, slope / 0.5) * 30)

    # Acceleration component (0-30)
    accel_score = min(30, max(0, (accel + 0.1) / 0.2) * 30)

    return int(round(vol_score + slope_score + accel_score))


# ==================== SCANNING ====================

def screen_felix_from_df(ticker: str, df: pd.DataFrame) -> Optional[Tuple[str, Dict]]:
    """
    Check a single ticker for a Felix Strategy signal using a pre-loaded DataFrame.
    """
    try:
        if df is None or df.empty:
            return None
        result = detect_felix_signal(df, ticker)
        if result["detected"]:
            return (ticker, result)
        return None
    except Exception as e:
        print(f"❌ Error screening {ticker}: {e}")
        return None


async def screen_felix(ticker: str, session: aiohttp.ClientSession) -> Optional[Tuple[str, Dict]]:
    """
    Legacy async wrapper - Check a single ticker for a Felix Strategy signal.
    """
    try:
        df = await get_polygon_data(ticker, session)
        if df is None:
            return None
        result = detect_felix_signal(df, ticker)
        if result["detected"]:
            return (ticker, result)
        return None
    except Exception as e:
        print(f"❌ Error screening {ticker}: {e}")
        return None


async def felix_strategy(session: aiohttp.ClientSession = None) -> List[Tuple[str, Dict]]:
    """
    Screen all S&P 500 tickers for Felix Strategy signals.
    Uses DB batch read for speed, falls back to API for missing tickers.
    """
    import time
    tickers = await get_sp500_tickers()

    # --- DB batch read ---
    t0 = time.time()
    dfs = get_multiple_dataframes_from_db(tickers)
    db_hits = sum(1 for v in dfs.values() if v is not None and not v.empty)
    print(f"📂 DB batch read: {db_hits}/{len(tickers)} tickers in {time.time()-t0:.2f}s")

    results = []
    api_fallback = []

    for t in tickers:
        df = dfs.get(t)
        if df is not None and not df.empty:
            r = screen_felix_from_df(t, df)
            if r:
                results.append(r)
        else:
            api_fallback.append(t)

    if api_fallback and session:
        print(f"🌐 API fallback for {len(api_fallback)} tickers...")
        tasks = [screen_felix(t, session) for t in api_fallback]
        api_results = await asyncio.gather(*tasks)
        results.extend([r for r in api_results if r is not None])

    return results


async def run_and_store_felix():
    """
    Execute the Felix Strategy scanner, print results, and persist to the database.
    """
    print("🚀 Felix Strategy – Institutional 50-SMA Breakout Scanner")
    print("=" * 65)
    print("Scanning S&P 500 for:")
    print("  • Price crossing above 50-SMA in last 3 trading days")
    print("  • 50-SMA curving upward (institutional trend)")
    print("  • Volume spike confirming big-buyer demand")
    print("=" * 65)

    db = SessionLocal()
    try:
        print("\n📊 Fetching S&P 500 tickers...")
        tickers = await get_sp500_tickers()
        print(f"✅ Got {len(tickers)} tickers to scan\n")

        print("🔍 Analysing stocks for Felix signals...")
        import time as _time
        t_start = _time.time()
        async with aiohttp.ClientSession() as aio_session:
            picks = await felix_strategy(aio_session)
        print(f"Total screening took {_time.time()-t_start:.2f}s")

        now = datetime.now()
        today = now.date()
        time_now = now.time().replace(microsecond=0)

        print(f"\n{'='*65}")
        print(f"✨ FELIX STRATEGY RESULTS – {today} {time_now}")
        print(f"{'='*65}")

        if picks:
            # Sort by signal quality descending
            picks.sort(key=lambda p: p[1].get("signal_quality", 0), reverse=True)

            print(f"\n🎯 Found {len(picks)} stocks with institutional 50-SMA breakout signals:\n")
            for idx, (sym, d) in enumerate(picks, 1):
                quality = d.get("signal_quality", 0)
                strength = d.get("institutional_strength", "N/A")
                vol_ratio = d.get("volume_ratio", 0)
                sma_slope = d.get("sma50_slope", 0)
                cross_dt = d.get("cross_date", "N/A")
                price = d.get("current_price", 0)

                print(
                    f"   {idx:2d}. {'🔥' if quality >= 70 else '📈'} {sym:6s} | "
                    f"Quality: {quality:3d} | "
                    f"Strength: {strength:11s} | "
                    f"Vol: {vol_ratio:.1f}× | "
                    f"Slope: {sma_slope:+.4f} | "
                    f"Cross: {cross_dt} | "
                    f"Price: ${price:>8.2f}"
                )

                entry = FelixData(
                    symbol=sym,
                    data_date=today,
                    data_time=time_now,
                    data_json=json.dumps(d),
                )
                db.add(entry)

            db.commit()
            print(f"\n💾 Database: Saved {len(picks)} Felix signals")
            print(f"{'='*65}")
            print(f"✅ Analysis complete – {len(picks)} institutional breakout candidates identified")
        else:
            print("\n⚠️  No Felix signals found today")
            print("   No S&P 500 stock crossed its 50-SMA with conviction volume")
            print("   in the last 3 trading days.")

        print(f"{'='*65}\n")
        return picks

    except Exception as e:
        print(f"\n❌ Error in run_and_store_felix: {e}")
        db.rollback()
        return []
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(run_and_store_felix())
