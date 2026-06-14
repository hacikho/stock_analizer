"""
Periodic task scheduling configuration for Celery Beat

NOTE: Uses app.conf.beat_schedule (set at module load time) instead of
on_after_configure.connect. The signal-based approach was unreliable because
the app is fully configured before start_celery_beat.py imports this module,
so the signal had already fired and tasks were never registered with Beat.
"""
from celery.schedules import crontab
from aignitequant.tasks.celery_app import app

# ============================================================
# Beat schedule -- assigned at module import time so tasks are
# always registered when start_celery_beat.py imports this module.
# ============================================================
app.conf.beat_schedule = {

    # --------------------------------------------------------
    # MARKET PULSE -- MOVED OUT OF CELERY BEAT.
    # The market-pulse snapshot is now refreshed by a single in-process
    # loop in aignitequant/app/main.py (_market_pulse_loop), which is
    # market-hours aware (30s while open, 240s while closed). It used to
    # run BOTH here every 30s AND in the API, double-fetching 24/7.
    # The 'aignitequant.tasks.fetch_market_pulse' task still exists for
    # manual/debug use; it just isn't scheduled here anymore.
    # --------------------------------------------------------

    # --------------------------------------------------------
    # MARKET DATA FETCH -- every 10 min, 4 AM - 8 PM ET
    # Must run before strategy tasks; populates market_data table.
    # --------------------------------------------------------
    'market-data-fetch-every-10min': {
        'task': 'aignitequant.tasks.fetch_market_data',
        'schedule': crontab(minute='*/10', hour='4-20', day_of_week='1-5'),
    },

    # --------------------------------------------------------
    # INTRADAY DATA FETCH -- 10-min bars, 4 AM - 8 PM ET
    # Covers pre-market, regular, and after-hours sessions.
    # --------------------------------------------------------
    'intraday-data-fetch-every-10min': {
        'task': 'aignitequant.tasks.fetch_intraday_data',
        'schedule': crontab(minute='*/10', hour='4-19', day_of_week='1-5'),
    },

    # --------------------------------------------------------
    # FAST STRATEGY TASKS -- every 15 min, 4 AM - 8 PM ET
    # Intraday/fast-moving signals that genuinely benefit from a 15-min
    # refresh. Kept at */15.
    # --------------------------------------------------------
    'canslim-every-15min': {
        'task': 'aignitequant.tasks.run_canslim',
        'schedule': crontab(minute='*/15', hour='4-20', day_of_week='1-5'),
    },
    'options-every-15min': {
        'task': 'aignitequant.tasks.run_option_strategies',
        'schedule': crontab(minute='*/15', hour='4-20', day_of_week='1-5'),
    },
    'bora-every-15min': {
        'task': 'aignitequant.tasks.run_bora_strategy',
        'schedule': crontab(minute='*/15', hour='4-20', day_of_week='1-5'),
    },

    # --------------------------------------------------------
    # SLOW STRATEGY TASKS -- hourly, 4 AM - 8 PM ET
    # Daily/swing signals (golden cross, stage 2, VCP, follow-the-money,
    # earnings quality) barely change within an hour, so running them
    # every 15 min just reloads the full S&P 500 into memory 4x as often.
    # Now hourly and staggered across the hour to flatten the worker's
    # memory peaks instead of spiking all at once.
    # --------------------------------------------------------
    'golden-cross-hourly': {
        'task': 'aignitequant.tasks.run_golden_cross',
        'schedule': crontab(minute=5, hour='4-20', day_of_week='1-5'),
    },
    'stage2-hourly': {
        'task': 'aignitequant.tasks.run_stage2',
        'schedule': crontab(minute=20, hour='4-20', day_of_week='1-5'),
    },
    'vcp-scanner-hourly': {
        'task': 'aignitequant.tasks.run_vcp_scanner',
        'schedule': crontab(minute=35, hour='4-20', day_of_week='1-5'),
    },
    'follow-the-money-hourly': {
        'task': 'aignitequant.tasks.run_follow_the_money',
        'schedule': crontab(minute=50, hour='4-20', day_of_week='1-5'),
    },
    'earnings-quality-hourly': {
        'task': 'aignitequant.tasks.run_earnings_quality',
        'schedule': crontab(minute=10, hour='4-20', day_of_week='1-5'),
    },

    # Felix, Vibia Hybrid, and Marios Swing were missing from the
    # old schedule -- added here so they actually run.
    'felix-every-15min': {
        'task': 'aignitequant.tasks.run_felix_strategy',
        'schedule': crontab(minute='*/15', hour='4-20', day_of_week='1-5'),
    },
    'vibia-hybrid-every-15min': {
        'task': 'aignitequant.tasks.run_vibia_hybrid',
        'schedule': crontab(minute='*/15', hour='4-20', day_of_week='1-5'),
    },
    'marios-swing-every-15min': {
        'task': 'aignitequant.tasks.run_marios_swing',
        'schedule': crontab(minute='*/15', hour='4-20', day_of_week='1-5'),
    },

    # --------------------------------------------------------
    # EARNINGS QUALITY -- also daily at 6:00 AM ET (pre-market)
    # All previous trading day's data is finalized; results ready
    # before market open for trading decisions.
    # WHY NOT 9:30 AM: opening volatility skews analysis.
    # --------------------------------------------------------
    'earnings-quality-daily-6am': {
        'task': 'aignitequant.tasks.run_earnings_quality',
        'schedule': crontab(minute=0, hour=6, day_of_week='1-5'),
    },

    # Follow The Money sector tasks replaced by the hourly run above.
}
