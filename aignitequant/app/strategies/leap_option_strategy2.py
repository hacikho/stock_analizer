"""
LEAP Option Strategy 2 Module
-----------------------------
Implements a LEAP option strategy for QQQ:
Entry: On days when QQQ gaps down by at least 2% (today's open at least 2% below yesterday's close),
	   buy to open two 60 Delta call options with 12 months expiration and a 50% profit lock.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime

def get_qqq_gap_down_leap_signal():
	"""
	Checks QQQ for LEAP call option signal based on the following rules:
	- QQQ gaps down >= 2% (today's open at least 2% below yesterday's close)
	If condition is met, returns a signal dict for buying two 60 Delta call options (12 months expiration, 50% profit lock).
	"""
	# Download last 3 days of QQQ data to ensure we have yesterday and today
	qqq = yf.Ticker('QQQ')
	df = qqq.history(period='3d')
	if df.empty or len(df) < 2:
		return None
	df = df.rename(columns={"Close": "close", "Open": "open"})

	today = df.index[-1]
	prev = df.index[-2]
	prev_close = df.loc[prev, 'close']
	today_open = df.loc[today, 'open']
	gap_pct = (today_open - prev_close) / prev_close

	if gap_pct <= -0.02:
		signal = {
			'symbol': 'QQQ',
			'type': 'LEAP Call',
			'delta': 0.60,
			'expiration': '12 months',
			'contracts': 2,
			'profit_lock': '50%',
			'date': str(today.date()),
			'note': 'Buy to open 2x 60 Delta call options, 12 months expiration, 50% profit lock.'
		}
		return signal
	return None

if __name__ == "__main__":
	signal = get_qqq_gap_down_leap_signal()
	if signal:
		print("LEAP Option Gap Down Signal:")
		print(signal)
	else:
		print("No LEAP option gap down signal for QQQ today.")
