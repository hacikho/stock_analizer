from celery import Celery
from celery.schedules import crontab
import pytz
import subprocess
import datetime

# Configure Celery app
app = Celery(
    'scheduler',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)

# US Eastern Timezone
EASTERN = pytz.timezone('US/Eastern')

@app.task
def run_option_strategies():
    # Run the run_option_strategies.py script as a subprocess
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running option strategies at {now}")
    result = subprocess.run([
        'python',
        'stock_analizer/app/strategies/run_option_strategies.py'
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("[Celery][ERROR]", result.stderr)
    return result.returncode
from celery import Celery
from celery.schedules import crontab
import pytz
import subprocess
import datetime

# Configure Celery app
app = Celery(
    'scheduler',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)

# US Eastern Timezone
EASTERN = pytz.timezone('US/Eastern')

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Every 30 minutes from 9:30am to 4:00pm ET, only on weekdays (Mon-Fri)
    # Celery's crontab does not support minute ranges with offsets, so we use two rules
    # 1. 30th minute of every hour from 9 to 15 (9:30, 10:30, ..., 15:30)
    # 2. 0th minute of every hour from 10 to 16 (10:00, 11:00, ..., 16:00)
    # CANSLIM
    sender.add_periodic_task(
        crontab(minute=30, hour='9-15', day_of_week='1-5'),
        run_canslim.s(),
        name='Run CANSLIM at :30 past the hour (market hours)'
    )
    sender.add_periodic_task(
        crontab(minute=0, hour='10-16', day_of_week='1-5'),
        run_canslim.s(),
        name='Run CANSLIM at :00 past the hour (market hours)'
    )
    # Option strategies
    sender.add_periodic_task(
        crontab(minute=30, hour='9-15', day_of_week='1-5'),
        run_option_strategies.s(),
        name='Run Option Strategies at :30 past the hour (market hours)'
    )
    sender.add_periodic_task(
        crontab(minute=0, hour='10-16', day_of_week='1-5'),
        run_option_strategies.s(),
        name='Run Option Strategies at :00 past the hour (market hours)'
    )

@app.task
def run_canslim():
    # Run the canslim_strategy.py script as a subprocess
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running CANSLIM strategy at {now}")
    result = subprocess.run([
        'python',
        'stock_analizer/app/strategies/canslim_strategy.py'
    ], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("[Celery][ERROR]", result.stderr)
    return result.returncode
