"""
LEAP Option Strategy Module
--------------------------
Implements a LEAP option strategy for QQQ:
1. If QQQ is down by at least 1% today, and QQQ is above its 100-day simple moving average (SMA),
   and the market is not in a bear market (bull market),
   then signal a buy to open a 60 Delta call option with 12 months expiration and a 50% profit lock.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def is_bull_market(qqq_df: pd.DataFrame) -> bool:
	"""
	Determines if the market is in a bull market (not a bear market).
	For simplicity, define bear market as QQQ closing 20% below its 52-week high.
	"""
	high_52w = qqq_df['close'].rolling(window=252, min_periods=1).max().iloc[-1]
	last_close = qqq_df['close'].iloc[-1]
	return last_close >= 0.8 * high_52w

def get_qqq_leap_signal():
	"""
	Checks QQQ for LEAP call option signal based on the following rules:
	- QQQ is down >= 1% today
	- QQQ is above its 100-day SMA
	- Market is not in a bear market (bull market)
	If all conditions are met, returns a signal dict for a 60 Delta call option (12 months expiration, 50% profit lock).
	"""
	# Download last 120 days of QQQ data
	qqq = yf.Ticker('QQQ')
	df = qqq.history(period='130d')
	if df.empty or len(df) < 100:
		return None
	df = df.rename(columns={"Close": "close"})

	# Calculate today's % change
	today = df.index[-1]
	prev = df.index[-2]
	pct_change = (df.loc[today, 'close'] - df.loc[prev, 'close']) / df.loc[prev, 'close']

	# Calculate 100-day SMA
	sma_100 = df['close'].rolling(window=100).mean().iloc[-1]
	above_100sma = df['close'].iloc[-1] > sma_100

	# Bull market check
	bull = is_bull_market(df)

	if pct_change <= -0.01 and above_100sma and bull:
		signal = {
			'symbol': 'QQQ',
			'type': 'LEAP Call',
			'delta': 0.60,
			'expiration': '12 months',
			'profit_lock': '50%',
			'date': str(today.date()),
			'note': 'Buy to open 60 Delta call option, 12 months expiration, 50% profit lock.'
		}
		return signal
	return None

if __name__ == "__main__":
	signal = get_qqq_leap_signal()
	if signal:
		print("LEAP Option Signal:")
		print(signal)
	else:
		print("No LEAP option signal for QQQ today.")
