import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
"""
Runs all options trading strategies and stores their signals in the OptionSignalData table.
"""
import json
from datetime import datetime
from app.db import SessionLocal, OptionSignalData
from app.strategies.leap_option_strategy1 import get_qqq_leap_signal
from app.strategies.leap_option_stategy2 import get_qqq_gap_down_leap_signal

def run_and_store_option_signals():
    session = SessionLocal()
    now = datetime.now()
    today = now.date()
    time_now = now.time().replace(microsecond=0)
    strategies = [
        ("leap_option_qqq", get_qqq_leap_signal),
        ("leap_option_qqq_gap", get_qqq_gap_down_leap_signal),
        # Add more strategies here as you grow
    ]
    inserted = 0
    for name, func in strategies:
        signal = func()
        if signal:
            entry = OptionSignalData(
                strategy=name,
                symbol=signal.get("symbol"),
                data_date=today,
                data_time=time_now,
                data_json=json.dumps(signal),
            )
            session.add(entry)
            inserted += 1
    session.commit()
    session.close()
    print(f"Inserted {inserted} option signals at {today} {time_now}")

if __name__ == "__main__":
    run_and_store_option_signals()
