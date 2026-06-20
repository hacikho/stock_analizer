"""
US (NYSE) market calendar helpers -- zero external dependencies.

Celery Beat crontabs already gate scheduled work to weekdays and to the
4 AM - 8 PM ET extended-hours window. What a crontab *cannot* express is
"skip US market holidays", so this module supplies:

  * is_market_holiday(d)      -> True on a full NYSE closure
  * is_market_open(now)       -> weekday AND not a holiday AND 4 AM-8 PM ET
  * skip_on_market_holiday    -> decorator that makes a Celery task return
                                 early (without doing any work) on holidays

Holidays are hardcoded (full-day NYSE closures). This avoids pulling in a
heavy calendar dependency on a memory-constrained Hobby plan. **Update the
HOLIDAYS set once a year** -- dates beyond the table fail OPEN (treated as a
normal trading day) so the app never silently stops running.
"""
import datetime
import functools

import pytz

EASTERN = pytz.timezone("US/Eastern")

# Extended-hours trading window (ET). Pre-market opens 4 AM; after-hours
# closes 8 PM. Hour 20 (8 PM) is treated as closed.
_OPEN_HOUR = 4
_CLOSE_HOUR = 20

# Full-day NYSE closures. NOTE: NYSE observes Good Friday and shifts holidays
# that fall on a weekend to the adjacent weekday. Update annually.
HOLIDAYS = {
    # ---- 2026 ----
    datetime.date(2026, 1, 1),    # New Year's Day
    datetime.date(2026, 1, 19),   # MLK Jr. Day
    datetime.date(2026, 2, 16),   # Washington's Birthday
    datetime.date(2026, 4, 3),    # Good Friday
    datetime.date(2026, 5, 25),   # Memorial Day
    datetime.date(2026, 6, 19),   # Juneteenth
    datetime.date(2026, 7, 3),    # Independence Day (observed; Jul 4 = Sat)
    datetime.date(2026, 9, 7),    # Labor Day
    datetime.date(2026, 11, 26),  # Thanksgiving
    datetime.date(2026, 12, 25),  # Christmas
    # ---- 2027 ----
    datetime.date(2027, 1, 1),    # New Year's Day
    datetime.date(2027, 1, 18),   # MLK Jr. Day
    datetime.date(2027, 2, 15),   # Washington's Birthday
    datetime.date(2027, 3, 26),   # Good Friday
    datetime.date(2027, 5, 31),   # Memorial Day
    datetime.date(2027, 6, 18),   # Juneteenth (observed; Jun 19 = Sat)
    datetime.date(2027, 7, 5),    # Independence Day (observed; Jul 4 = Sun)
    datetime.date(2027, 9, 6),    # Labor Day
    datetime.date(2027, 11, 25),  # Thanksgiving
    datetime.date(2027, 12, 24),  # Christmas (observed; Dec 25 = Sat)
}


def _eastern_now(now=None) -> datetime.datetime:
    if now is None:
        return datetime.datetime.now(EASTERN)
    if now.tzinfo is None:
        return EASTERN.localize(now)
    return now.astimezone(EASTERN)


def is_market_holiday(d=None) -> bool:
    """True if the given date (ET, defaults to today) is a full NYSE closure."""
    if d is None:
        d = _eastern_now().date()
    elif isinstance(d, datetime.datetime):
        d = _eastern_now(d).date()
    return d in HOLIDAYS


def is_market_open(now=None) -> bool:
    """
    True during US extended trading hours: Mon-Fri, not a market holiday,
    between 4 AM and 8 PM ET.
    """
    now = _eastern_now(now)
    if now.weekday() >= 5:            # Saturday / Sunday
        return False
    if is_market_holiday(now.date()):  # New Year's, July 4th, etc.
        return False
    return _OPEN_HOUR <= now.hour < _CLOSE_HOUR


def skip_on_market_holiday(task_fn):
    """
    Decorator for Celery tasks: if today is a US market holiday, log and
    return a 'skipped' result instead of running the task. Beat crontabs
    already exclude weekends and off-hours, so this adds only the holiday
    exclusion that a crontab cannot express.

    Place BELOW @app.task so the registered task includes the guard:

        @app.task(name='...')
        @skip_on_market_holiday
        def run_strategy():
            ...
    """
    @functools.wraps(task_fn)
    def wrapper(*args, **kwargs):
        if is_market_holiday():
            today = _eastern_now().date().isoformat()
            print(f"[Celery] Skipping {task_fn.__name__}: {today} is a US market holiday.")
            return {"status": "skipped", "reason": "market_holiday", "date": today}
        return task_fn(*args, **kwargs)
    return wrapper
