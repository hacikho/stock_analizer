"""
Periodic task scheduling configuration for Celery Beat
"""
from celery.schedules import crontab
from aignitequant.tasks.celery_app import app


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """
    Configure periodic tasks to run during market hours
    Market hours: 9:30 AM - 4:00 PM ET, Monday-Friday
    Tasks run every 30 minutes during market hours
    """
    
    # ============================================================
    # MARKET DATA FETCH — Every 10 minutes during market hours
    # This MUST run before any strategy tasks.
    # Populates the shared market_data table that all strategies read from.
    # ============================================================
    sender.add_periodic_task(
        crontab(minute='*/10', hour='9-16', day_of_week='1-5'),
        app.signature('aignitequant.tasks.fetch_market_data'),
        name='Market Data Fetch every 10 min (market hours)'
    )
    
    # Also run once at 6:00 AM to ensure pre-market strategies have fresh data
    sender.add_periodic_task(
        crontab(minute=0, hour=6, day_of_week='1-5'),
        app.signature('aignitequant.tasks.fetch_market_data'),
        name='Market Data Fetch at 6:00 AM (pre-market)'
    )
    
    # ============================================================
    # INTRADAY DATA FETCH — 10-minute bars, every 10 min
    # Covers all 3 sessions:
    #   Pre-market  : 4:00 AM – 9:29 AM ET  (hours 4-9)
    #   Regular     : 9:30 AM – 3:59 PM ET  (hours 9-15, already overlaps above)
    #   After-hours : 4:00 PM – 7:59 PM ET  (hours 16-19)
    # ============================================================
    sender.add_periodic_task(
        crontab(minute='*/10', hour='4-19', day_of_week='1-5'),
        app.signature('aignitequant.tasks.fetch_intraday_data'),
        name='Intraday 10-min bars (4 AM – 8 PM ET)'
    )
    
    # CANSLIM Strategy - Every 30 minutes during market hours
    # Runs at :30 and :00 of each hour
    sender.add_periodic_task(
        crontab(minute=30, hour='9-15', day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_canslim'),
        name='CANSLIM at :30 past hour (market hours)'
    )
    sender.add_periodic_task(
        crontab(minute=0, hour='10-16', day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_canslim'),
        name='CANSLIM at :00 past hour (market hours)'
    )
    
    # Option Strategies - Every 30 minutes during market hours
    sender.add_periodic_task(
        crontab(minute=30, hour='9-15', day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_option_strategies'),
        name='Options at :30 past hour (market hours)'
    )
    sender.add_periodic_task(
        crontab(minute=0, hour='10-16', day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_option_strategies'),
        name='Options at :00 past hour (market hours)'
    )
    
    # BORA Strategy - Every hour during market hours
    sender.add_periodic_task(
        crontab(minute=0, hour='10-16', day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_bora_strategy'),
        name='BORA at top of hour (market hours)'
    )
    
    # Golden Cross - Once per day before market open
    sender.add_periodic_task(
        crontab(minute=0, hour=9, day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_golden_cross'),
        name='Golden Cross daily pre-market'
    )
    
    # Stage 2 Analysis - Once per day after market close
    sender.add_periodic_task(
        crontab(minute=30, hour=16, day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_stage2'),
        name='Stage 2 daily post-market'
    )
    
    # VCP Scanner - Once per day after market close (takes ~10-15 minutes)
    sender.add_periodic_task(
        crontab(minute=0, hour=17, day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_vcp_scanner'),
        name='VCP Scanner daily post-market'
    )
    
    # Follow-The-Money Analysis - Every 15 minutes during market hours
    # Runs at :00, :15, :30, :45 of each hour during market hours (9:30 AM - 4:00 PM ET)
    sender.add_periodic_task(
        crontab(minute='0,15,30,45', hour='9', day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_follow_the_money'),
        name='Follow-The-Money at 9:00, 9:15, 9:30, 9:45 AM'
    )
    sender.add_periodic_task(
        crontab(minute='0,15,30,45', hour='10-15', day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_follow_the_money'),
        name='Follow-The-Money every 15 min (market hours)'
    )
    sender.add_periodic_task(
        crontab(minute='0', hour='16', day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_follow_the_money'),
        name='Follow-The-Money at market close (4:00 PM)'
    )
    
    # Earnings Quality Analysis - Once per day at 6:00 AM ET (pre-market)
    # SCHEDULE JUSTIFICATION:
    # - Runs at 6:00 AM ET Monday-Friday (3.5 hours before market open)
    # - All previous trading day's data is finalized and complete
    # - After-hours earnings (4-8 PM) have 10+ hours to settle
    # - Polygon Starter plan has 15-min delay, but daily bars are historical (no delay)
    # - Analysis captures last 2-3 trading days of earnings (today + 2 prior trading days)
    # - Results ready BEFORE market opens for trading decisions
    # - Database caching prevents redundant API calls if run multiple times same day
    # - After-hours earnings naturally flow into next day's price action
    # - Optimal timing: Yesterday's close is 14 hours old, all data reliable
    # - Yahoo Finance earnings calendar updated overnight
    # WHY NOT 9:30 AM: Opening volatility skews analysis, incomplete overnight data
    # WHY NOT 5:30 PM: Good alternative, but morning timing better for pre-market planning
    sender.add_periodic_task(
        crontab(minute=0, hour=6, day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_earnings_quality'),
        name='Earnings Quality daily at 6:00 AM ET (pre-market)'
    )    
    # Follow The Money - Sector Rotation Analysis (3x per trading day)
    # WHY 3 TIMES PER DAY:
    # - Sector rotation happens gradually (days/weeks), not minute-by-minute
    # - 3 snapshots capture opening, midday, and closing dynamics
    # - Balances freshness with API rate limits (100+ calls per run)
    # 
    # Schedule:
    # 9:45 AM ET - After opening volatility settles, first update of the day
    # 12:30 PM ET - Midday update captures lunch hour trends
    # 4:15 PM ET - Post-close with complete trading day data
    sender.add_periodic_task(
        crontab(minute=45, hour=9, day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_follow_the_money_sector'),
        name='Follow The Money at 9:45 AM ET (market open)'
    )
    sender.add_periodic_task(
        crontab(minute=30, hour=12, day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_follow_the_money_sector'),
        name='Follow The Money at 12:30 PM ET (midday)'
    )
    sender.add_periodic_task(
        crontab(minute=15, hour=16, day_of_week='1-5'),
        app.signature('aignitequant.tasks.run_follow_the_money_sector'),
        name='Follow The Money at 4:15 PM ET (market close)'
    )