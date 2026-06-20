#!/usr/bin/env python
"""
Start the Celery worker with Beat embedded (-B).

Two modes
---------
* Default (always-on):
    Long-running worker + embedded beat, exactly as before. Beat's crontabs
    already gate task execution to 4 AM-8 PM ET, Mon-Fri, and the
    @skip_on_market_holiday guard skips US market holidays.

* Scale-to-zero (set env WORKER_SCALE_TO_ZERO=1):
    Intended to be paired with a Railway **Cron Schedule** of  `0 8 * * 1-5`
    (UTC = ~4 AM ET on weekdays). Railway starts this service each market
    morning; the worker then:
      - exits immediately (status 0) on weekends / US market holidays, so the
        container is not billed on non-market days, and
      - self-terminates at 8 PM ET, so the container is not billed overnight.
    Railway's cron restarts it the next market morning. The clean daily
    restart also caps the worker's memory growth.

WARNING: only enable WORKER_SCALE_TO_ZERO together with the Railway Cron
    Schedule. Without the cron trigger, nothing would restart the worker
    after it self-terminates.
"""
import os
import sys
import time
import signal
import threading
import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aignitequant.tasks.celery_app import app
# Importing the scheduler registers app.conf.beat_schedule for the embedded
# beat (-B). Without this import the embedded scheduler starts empty.
from aignitequant.tasks import scheduler  # noqa: F401
from aignitequant.market_calendar import EASTERN, is_market_holiday

SCALE_TO_ZERO = os.getenv("WORKER_SCALE_TO_ZERO", "0") == "1"
MARKET_CLOSE_HOUR = 20  # 8 PM ET -- end of the extended-hours session


def _is_non_market_day(now=None) -> bool:
    now = now or datetime.datetime.now(EASTERN)
    return now.weekday() >= 5 or is_market_holiday(now.date())


def _shutdown_at_market_close():
    """
    Daemon thread: once 8 PM ET passes, ask Celery to shut down gracefully
    (SIGTERM -> warm shutdown finishes any in-flight task and closes broker /
    DB connections), then hard-exit as a fallback. Exit status 0 so Railway
    treats the cron run as completed and waits for the next schedule.
    """
    while True:
        now = datetime.datetime.now(EASTERN)
        if now.hour >= MARKET_CLOSE_HOUR:
            print(f"[worker] {now:%Y-%m-%d %H:%M} ET -- session over; "
                  f"shutting down to scale to zero until the next cron start.")
            os.kill(os.getpid(), signal.SIGTERM)  # graceful warm shutdown
            time.sleep(30)
            os._exit(0)                            # fallback hard exit
        time.sleep(60)


def main():
    print("Starting Celery worker (Beat embedded via -B)...")
    print("Beat fires tasks 4 AM-8 PM ET, Mon-Fri, excluding US market holidays.")

    if SCALE_TO_ZERO:
        if _is_non_market_day():
            print("[worker] Non-market day (weekend/holiday) -- exiting "
                  "immediately for scale-to-zero.")
            sys.exit(0)
        threading.Thread(target=_shutdown_at_market_close, daemon=True).start()
        print("[worker] Scale-to-zero mode ON: self-terminates at 8 PM ET.")
    else:
        print("[worker] Always-on mode (set WORKER_SCALE_TO_ZERO=1 + a Railway "
              "Cron Schedule to enable scale-to-zero).")

    # Start the worker WITH embedded Beat (-B); solo pool is proven in this
    # deployment. A separate celery-beat service is no longer needed.
    app.worker_main([
        'worker',
        '--loglevel=info',
        '--pool=solo',
        '--beat',
    ])


if __name__ == '__main__':
    main()
