#!/usr/bin/env python3
"""
canslim_screener.py

Basic CANSLIM stock screener using Yahoo Finance data via yfinance.

Usage:
    python canslim_screener.py --tickers AAPL MSFT AMZN TSLA
"""

import argparse
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ----------------------------
#  Helper functions
# ----------------------------

def fetch_price_data(ticker, period="1y", interval="1d"):
    """Download OHLCV price data for given ticker."""
    return yf.Ticker(ticker).history(period=period, interval=interval)

def recent_earnings_growth(ticker):
    """Compute Q/Q earnings growth (%) for the most recent quarter."""
    q = yf.Ticker(ticker).quarterly_earnings
    if q.shape[0] < 2:
        return np.nan
    # assume DataFrame indexed by year-quarter descending
    latest, prior = q["Earnings"].iloc[0], q["Earnings"].iloc[1]
    return (latest - prior) / abs(prior) * 100

def annual_earnings_growth(ticker):
    """Compute Y/Y earnings growth (%) for the most recent fiscal year."""
    a = yf.Ticker(ticker).earnings
    if a.shape[0] < 2:
        return np.nan
    latest, prior = a["Earnings"].iloc[0], a["Earnings"].iloc[1]
    return (latest - prior) / abs(prior) * 100

def is_near_52w_high(df, threshold=0.95):
    """Return True if latest close is within threshold of 52‑week high."""
    high_52 = df["High"].max()
    last_close = df["Close"].iloc[-1]
    return last_close >= high_52 * threshold

def volume_spike(df, window_short=20, window_long=50, pct_increase=30):
    """True if recent avg volume (20d) is pct_increase above 50d avg."""
    vol = df["Volume"]
    if len(vol) < window_long:
        return False
    recent = vol.tail(window_short).mean()
    prior  = vol.tail(window_long).head(window_long-window_short).mean()
    return (recent - prior) / prior * 100 >= pct_increase

def relative_strength(ticker, market="^GSPC", period="6mo"):
    """Compute relative strength: price gain(ticker) ÷ price gain(market)."""
    df_t = fetch_price_data(ticker, period=period)
    df_m = fetch_price_data(market, period=period)
    if df_t.empty or df_m.empty: return np.nan
    gain_t = df_t["Close"].iloc[-1] / df_t["Close"].iloc[0] - 1
    gain_m = df_m["Close"].iloc[-1] / df_m["Close"].iloc[0] - 1
    return gain_t / gain_m if gain_m != 0 else np.nan

def institutional_holders_count(ticker):
    """Return number of institutional holders from Yahoo Finance."""
    holders = yf.Ticker(ticker).institutional_holders
    return len(holders) if holders is not None else 0

def market_trend_ok(market="^GSPC", short_ma=50, long_ma=200):
    """Check if market is in an up‑trend: 50‑day MA > 200‑day MA."""
    df = fetch_price_data(market, period="1y")
    if len(df) < long_ma:
        return False
    ma50 = df["Close"].rolling(short_ma).mean().iloc[-1]
    ma200 = df["Close"].rolling(long_ma).mean().iloc[-1]
    return ma50 > ma200

# ----------------------------
#  Main screening logic
# ----------------------------

def screen_ticker(ticker):
    info = {"Ticker": ticker}
    try:
        df = fetch_price_data(ticker)
        # C: Quarterly earnings growth
        info["QtrEarningsGrowth%"] = recent_earnings_growth(ticker)
        # A: Annual earnings growth
        info["YrEarningsGrowth%"] = annual_earnings_growth(ticker)
        # N: New/high price
        info["Near52WHigh"] = is_near_52w_high(df)
        # S: Volume spike
        info["VolumeUp30%"] = volume_spike(df)
        # L: Relative strength
        info["RelStrength"] = relative_strength(ticker)
        # I: Institutional holders
        info["InstHolders"] = institutional_holders_count(ticker)
    except Exception as e:
        info["Error"] = str(e)
    return info

def canslim_screen(tickers):
    """Run CANSLIM filter on list of tickers. Returns DataFrame of survivors."""
    # Market trend filter
    if not market_trend_ok():
        print("Market not in up‑trend (50d MA <= 200d MA). Aborting screen.")
        return pd.DataFrame()
    rows = []
    for t in tickers:
        row = screen_ticker(t)
        # apply hard filters
        if (
            row.get("QtrEarningsGrowth%", 0)  >= 25 and
            row.get("YrEarningsGrowth%", 0)  >= 25 and
            row.get("Near52WHigh", False)    and
            row.get("VolumeUp30%", False)    and
            row.get("RelStrength", 0)        >= 1.0 and
            row.get("InstHolders", 0)        >= 3
        ):
            rows.append(row)
    return pd.DataFrame(rows).sort_values("RelStrength", ascending=False)

# ----------------------------
#  Command line interface
# ----------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CANSLIM Stock Screener")
    parser.add_argument(
        "--tickers", "-t", nargs="+", required=True,
        help="List of stock tickers to screen"
    )
    args = parser.parse_args()

    df_result = canslim_screen(args.tickers)
    if df_result.empty:
        print("No stocks passed the CANSLIM filters.")
    else:
        print("\nStocks passing CANSLIM criteria:\n")
        print(df_result.to_markdown(index=False))


#python canslim_screener.py -t AAPL MSFT AMZN NVDA TSLA
