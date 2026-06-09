"""
Periodic task scheduling configuration for Celery Beat

NOTE: Uses app.conf.beat_schedule (set at module load time) instead of
on_after_configure.connect. The signal-based approach was unreliable because
the app is fully configured before start_celery_beat.py imports this module,
so the signal had already fired and tasks were never registered with Beat.
"""
from datetime import timedelta
from celery.schedules import crontab
from aignitequant.tasks.celery_app import app

# ============================================================
# Beat schedule -- assigned at module import time so tasks are
# always registered when start_celery_beat.py imports this module.
# ============================================================
app.conf.beat_schedule = {

    # --------------------------------------------------------
    # MARKET PULSE -- every 30 seconds, always-on
    # Snapshots for 8 macro instruments: SPY (S&P 500), QQQ (NASDAQ),
    # DIA (Dow 30), IWM (Russell 2000), VXX (VIX proxy), GLD (Gold),
    # X:BTCUSD (Bitcoin), USO (Crude Oil).
    # Uses timedelta (not crontab) for sub-minute scheduling.
    # 2-3 lightweight Polygon API calls per run (~8/min total).
    # Data age is always < 30s; frontend should poll every 10-30s.
    # --------------------------------------------------------
    'market-pulse-every-30sec': {
        'task': 'aignitequant.tasks.fetch_market_pulse',
        'schedule': timedelta(seconds=30),
    },

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
    # STRATEGY TASKS -- every 15 min, 4 AM - 8 PM ET
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
    'golden-cross-every-15min': {
        'task': 'aignitequant.tasks.run_golden_cross',
        'schedule': crontab(minute='*/15', hour='4-20', day_of_week='1-5'),
    },
    'stage2-every-15min': {
        'task': 'aignitequant.tasks.run_stage2',
        'schedule': crontab(minute='*/15', hour='4-20', day_of_week='1-5'),
    },
    'vcp-scanner-every-15min': {
        'task': 'aignitequant.tasks.run_vcp_scanner',
        'schedule': crontab(minute='*/15', hour='4-20', day_of_week='1-5'),
    },
    'follow-the-money-every-15min': {
        'task': 'aignitequant.tasks.run_follow_the_money',
        'schedule': crontab(minute='*/15', hour='4-20', day_of_week='1-5'),
    },
    'earnings-quality-every-15min': {
        'task': 'aignitequant.tasks.run_earnings_quality',
        'schedule': crontab(minute='*/15', hour='4-20', day_of_week='1-5'),
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

    # --------------------------------------------------------
    # FOLLOW THE MONEY SECTOR -- 3x per trading day
    # Sector rotation is gradual; 3 snapshots capture open,
    # midday, and closing dynamics without excessive API usage.
    # --------------------------------------------------------
    'follow-the-money-sector-945am': {
        'task': 'aignitequant.tasks.run_follow_the_money_sector',
        'schedule': crontab(minute=45, hour=9, day_of_week='1-5'),
    },
    'follow-the-money-sector-1230pm': {
        'task': 'aignitequant.tasks.run_follow_the_money_sector',
        'schedule': crontab(minute=30, hour=12, day_of_week='1-5'),
    },
    'follow-the-money-sector-415pm': {
        'task': 'aignitequant.tasks.run_follow_the_money_sector',
        'schedule': crontab(minute=15, hour=16, day_of_week='1-5'),
    },
}
