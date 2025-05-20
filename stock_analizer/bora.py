import yfinance as yf
import pandas as pd
import numpy as np

def fetch_data(symbol, period="300d", interval="1d"):
    # Pass symbol as a plain string, not a list,
    # and force group_by="column" so you never get a ticker‐level MultiIndex.
    return yf.download(
        symbol,
        period=period,
        interval=interval,
        auto_adjust=False,
        group_by="column",
        progress=False,
    )

def compute_indicators(df):
    """Add SMA_200, EMA_21, and EMA_50 columns to df."""
    df = df.copy()
    df["SMA_200"] = df["Close"].rolling(window=200).mean()
    df["EMA_21"]  = df["Close"].ewm(span=21, adjust=False).mean()
    df["EMA_50"]  = df["Close"].ewm(span=50, adjust=False).mean()
    return df

def ema21_trend_ok(df, lookback=10,
                   method="slope", slope_thresh=0.0,
                   pct_thresh=1.0):
    """
    Check EMA_21 trend over last `lookback` days by one of:
      - "slope": fit linear regression, require slope > slope_thresh
      - "pct": require percent gain over window > pct_thresh (%)
      - "strict": fallback to strictly increasing
    """
    recent = df["EMA_21"].dropna().iloc[-lookback:]
    if len(recent) < lookback:
        return False

    if method == "slope":
        # x = days 0..lookback-1, y = EMA values
        x = np.arange(lookback)
        y = recent.values
        slope, _ = np.polyfit(x, y, 1)
        return slope > slope_thresh

    elif method == "pct":
        pct_change = (recent.iloc[-1] / recent.iloc[0] - 1) * 100
        return pct_change > pct_thresh

    else:  # strict
        return all(x < y for x, y in zip(recent, recent[1:]))

import pandas as pd

def scan_symbols(symbols,
                 ema21_method="slope",
                 slope_thresh=0.0,
                 pct_thresh=1.0,
                 lookback=10):
    picks = []
    for sym in symbols:
        df = fetch_data(sym)

        # — if df has MultiIndex columns, pick out just this symbol’s sub‐DataFrame —
        if isinstance(df.columns, pd.MultiIndex):
            # try both levels in case your MultiIndex is (ticker, variable)
            if sym in df.columns.get_level_values(0):
                df = df.xs(sym, axis=1, level=0)
            elif sym in df.columns.get_level_values(1):
                df = df.xs(sym, axis=1, level=1)
            else:
                # fallback: drop the outer level entirely
                df.columns = df.columns.droplevel(0)

        if len(df) < 200:
            continue

        df = compute_indicators(df)

        # Extract the raw scalar values:
        price_last = df["Close"].iloc[-1]
        sma200_last = df["SMA_200"].iloc[-1]
        ema21_last = df["EMA_21"].iloc[-1]
        ema50_last = df["EMA_50"].iloc[-1]

        # If any of these are still a Series, pull out the first element
        for name, val in [("price", price_last),
                          ("SMA", sma200_last),
                          ("EMA21", ema21_last),
                          ("EMA50", ema50_last)]:
            if isinstance(val, pd.Series):
                locals()[f"{name}_last"] = val.iloc[0]  # overwrite with scalar

        # Now these are all floats; no more ambiguous truth tests:
        if price_last <= sma200_last:
            continue
        if ema21_last <= ema50_last:
            continue
        if not ema21_trend_ok(df,
                              lookback=lookback,
                              method=ema21_method,
                              slope_thresh=slope_thresh,
                              pct_thresh=pct_thresh):
            continue

        picks.append(sym)

    return picks



if __name__ == "__main__":
    watchlist = ["PYPL", "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META"]

    # Example 1: linear‐regression slope > 0
    winners_slope = scan_symbols(
        watchlist,
        ema21_method="slope",
        slope_thresh=0.0,
        lookback=10
    )
    print("By slope:", winners_slope)

    # Example 2: % gain > 2% over last 10 days
    winners_pct = scan_symbols(
        watchlist,
        ema21_method="pct",
        pct_thresh=2.0,
        lookback=10
    )
    print("By pct >2%:", winners_pct)

    # Example 3: strict increasing over last 5 days
    winners_strict = scan_symbols(
        watchlist,
        ema21_method="strict",
        lookback=5
    )
    print("By strict rising:", winners_strict)
