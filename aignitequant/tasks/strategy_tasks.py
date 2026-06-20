"""
Celery tasks for executing trading strategies
"""
import datetime
import pytz
from aignitequant.tasks.celery_app import app
from aignitequant.market_calendar import skip_on_market_holiday
from aignitequant.app.db import SessionLocal, OptionSignalData, CanSlimData
from aignitequant.app.services.events import publish_update


EASTERN = pytz.timezone('US/Eastern')


# ============================================================
# Market Data Fetch Task — runs every 10 minutes during market hours
# ============================================================
@app.task(name='aignitequant.tasks.fetch_market_data', time_limit=600)
@skip_on_market_holiday
def fetch_market_data():
    """
    Centralized market data fetch job.
    Pulls OHLCV data for all S&P 500 tickers from Polygon.io
    and stores it in the shared market_data table.
    
    All strategies then read from DB instead of calling the API.
    """
    import asyncio
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Starting Market Data Fetch at {now}")
    
    try:
        from aignitequant.app.services.market_data import fetch_all_market_data
        
        stats = asyncio.run(fetch_all_market_data(
            batch_size=5,
            delay=1.0,
            extra_tickers=["SPY", "QQQ", "TQQQ"],
        ))
        
        print(f"[Celery] Market Data Fetch complete: {stats}")
        return {"status": "success", "timestamp": now.isoformat(), **stats}
    
    except Exception as e:
        print(f"[Celery][ERROR] Market Data Fetch failed: {str(e)}")
        return {"status": "error", "error": str(e)}


# ============================================================
# Intraday Data Fetch Task — every 10 min, 4 AM – 8 PM ET
# Pulls 10-minute bars for pre-market, regular, and after-hours
# ============================================================
@app.task(name='aignitequant.tasks.fetch_intraday_data', time_limit=900)
@skip_on_market_holiday
def fetch_intraday_data():
    """
    Fetch 10-minute OHLCV bars for all S&P 500 tickers + extras.
    Covers pre-market (4 AM), regular hours, and after-hours (8 PM).
    Stores into intraday_bars table; prunes bars older than 5 days.
    """
    import asyncio
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Starting Intraday Data Fetch at {now}")
    
    try:
        from aignitequant.app.services.intraday_data import fetch_intraday_data as _fetch
        
        stats = asyncio.run(_fetch(
            batch_size=5,
            delay=1.0,
            extra_tickers=["SPY", "QQQ", "TQQQ"],
            retention_days=5,
        ))
        
        print(f"[Celery] Intraday Data Fetch complete: {stats}")
        return {"status": "success", "timestamp": now.isoformat(), **stats}
    
    except Exception as e:
        print(f"[Celery][ERROR] Intraday Data Fetch failed: {str(e)}")
        return {"status": "error", "error": str(e)}


# ============================================================
# Market Pulse Fetch Task — every 1 minute during market hours
# Pulls live snapshots for 8 macro instruments:
#   S&P 500 (SPY), NASDAQ (QQQ), Dow 30 (DIA), Russell 2000 (IWM),
#   VIX proxy (VXX), Gold (GLD), Bitcoin (X:BTCUSD), Crude Oil (USO)
# ============================================================
@app.task(name='aignitequant.tasks.fetch_market_pulse', time_limit=60)
def fetch_market_pulse():
    """
    Fetch live price snapshots for 8 macro instruments and upsert into
    the market_pulse table. 2-3 lightweight Polygon API calls total.
    Frontend consumes GET /api/market-pulse which reads from this table.
    """
    import asyncio

    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Starting Market Pulse Fetch at {now}")

    try:
        from aignitequant.app.services.market_pulse import fetch_and_store_market_pulse

        stats = asyncio.run(fetch_and_store_market_pulse())

        print(f"[Celery] Market Pulse Fetch complete: {stats}")
        return {"status": "success", "timestamp": now.isoformat(), **stats}

    except Exception as e:
        print(f"[Celery][ERROR] Market Pulse Fetch failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_option_strategies')
@skip_on_market_holiday
def run_option_strategies():
    """
    Execute option trading strategies and save results to database
    """
    import json
    from aignitequant.app.strategies.leap_option_strategy1 import get_qqq_leap_signal
    from aignitequant.app.strategies.leap_option_strategy2 import get_qqq_gap_down_leap_signal

    now = datetime.datetime.now(EASTERN)
    today = now.date()
    time_now = now.time().replace(microsecond=0)
    print(f"[Celery] Running option strategies at {now}")

    db = SessionLocal()
    saved = 0
    try:
        # Run strategy 1 — returns signal dict or None
        result1 = get_qqq_leap_signal()
        if result1 is not None:
            db.add(OptionSignalData(
                strategy="leap_option_qqq",
                symbol=result1.get("symbol", "QQQ"),
                data_date=today,
                data_time=time_now,
                data_json=json.dumps(result1),
            ))
            saved += 1

        # Run strategy 2 — returns signal dict or None
        result2 = get_qqq_gap_down_leap_signal()
        if result2 is not None:
            db.add(OptionSignalData(
                strategy="leap_option_qqq_gap",
                symbol=result2.get("symbol", "QQQ"),
                data_date=today,
                data_time=time_now,
                data_json=json.dumps(result2),
            ))
            saved += 1

        db.commit()
        print(f"[Celery] Option strategies completed: {saved} signals saved")
        publish_update("options")
        return {"status": "success", "signals": saved, "timestamp": now.isoformat()}

    except Exception as e:
        db.rollback()
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}
    finally:
        db.close()


@app.task(name='aignitequant.tasks.run_canslim')
@skip_on_market_holiday
def run_canslim():
    """
    Execute CANSLIM screening strategy
    """
    import asyncio
    from aignitequant.app.strategies.canslim_strategy import run_and_store_canslim

    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running CANSLIM strategy at {now}")

    try:
        asyncio.run(run_and_store_canslim())
        publish_update("canslim")
        return {"status": "success", "timestamp": now.isoformat()}

    except Exception as e:
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_bora_strategy')
@skip_on_market_holiday
def run_bora_strategy():
    """
    Execute BORA (Breakout Reversal Analysis) strategy
    """
    import asyncio
    from aignitequant.app.strategies.bora_strategy import run_and_store_bora, check_and_exit_positions

    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running BORA strategy at {now}")

    try:
        # Check exit signals on active positions first
        asyncio.run(check_and_exit_positions())
        # Then scan for new picks and save to DB
        count = asyncio.run(run_and_store_bora())
        print(f"[Celery] BORA completed: found {count} signals")
        publish_update("bora")
        return {"status": "success", "signals": count, "timestamp": now.isoformat()}

    except Exception as e:
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_golden_cross')
@skip_on_market_holiday
def run_golden_cross():
    """
    Execute Golden Cross strategy
    """
    import asyncio
    from aignitequant.app.strategies.golden_cross_strategy import run_and_store_golden_cross

    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Golden Cross strategy at {now}")

    try:
        picks = asyncio.run(run_and_store_golden_cross())
        count = len(picks) if picks else 0
        print(f"[Celery] Golden Cross completed: found {count} signals")
        publish_update("golden_cross")
        return {"status": "success", "signals": count, "timestamp": now.isoformat()}

    except Exception as e:
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_stage2')
@skip_on_market_holiday
def run_stage2():
    """
    Execute Stage 2 trend analysis
    """
    import asyncio
    from aignitequant.app.strategies.stage2 import run_and_store_stage2

    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Stage 2 analysis at {now}")

    try:
        qualified = asyncio.run(run_and_store_stage2())
        count = len(qualified) if qualified else 0
        print(f"[Celery] Stage 2 completed: found {count} candidates")
        publish_update("stage2")
        return {"status": "success", "candidates": count, "timestamp": now.isoformat()}

    except Exception as e:
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_vcp_scanner')
@skip_on_market_holiday
def run_vcp_scanner():
    """
    Execute VCP (Volatility Contraction Pattern) scanner on S&P 500
    """
    import asyncio
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running VCP scanner at {now}")
    
    try:
        # Import inside task to avoid import issues
        from aignitequant.app.strategies.vcp_scanner_strategy import scan_sp500_for_vcp, save_vcp_results_to_db
        
        # Run async VCP scan
        async def vcp_scan():
            results = await scan_sp500_for_vcp(batch_size=10, delay=2.0)
            return results
        
        # Execute async function
        results = asyncio.run(vcp_scan())
        
        # Save to database
        saved_count = save_vcp_results_to_db(results)
        
        print(f"[Celery] VCP scanner completed: found {saved_count} VCP candidates")
        publish_update("vcp")
        return {"status": "success", "candidates": saved_count, "timestamp": now.isoformat()}
    
    except Exception as e:
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_follow_the_money')
@skip_on_market_holiday
def run_follow_the_money():
    """
    Execute Follow-The-Money sector rotation analysis
    Analyzes institutional money flow, sector rotation, and individual stock leaders
    """
    import asyncio
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Follow-The-Money analysis at {now}")
    
    try:
        # Import inside task to avoid import issues
        from aignitequant.app.strategies.follow_the_money import main as run_analysis
        
        # Run async analysis
        results = asyncio.run(run_analysis())
        
        print(f"[Celery] Follow-The-Money analysis completed successfully")
        publish_update("follow_the_money")
        return {"status": "success", "timestamp": now.isoformat()}

    except Exception as e:
        print(f"[Celery][ERROR] Follow-The-Money failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_earnings_quality')
@skip_on_market_holiday
def run_earnings_quality():
    """
    Execute Earnings Quality Score analysis for stocks with recent earnings
    
    Analyzes stocks that reported earnings in the last 2-3 trading days and calculates
    a comprehensive quality score (0-100) based on:
    - Earnings beat/miss (revenue & EPS growth)
    - Guidance updates and analyst sentiment
    - Post-earnings price action and volume
    - Financial health metrics
    - Analyst coverage and recommendations
    
    Results are saved to database and can be queried for trading decisions.
    """
    import asyncio
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Earnings Quality analysis at {now}")
    
    try:
        # Import inside task to avoid import issues
        from aignitequant.app.strategies.earnings_quality_score import main as run_analysis
        
        # Run async analysis
        asyncio.run(run_analysis())
        
        print(f"[Celery] Earnings Quality analysis completed successfully")
        publish_update("earnings_quality")
        return {"status": "success", "timestamp": now.isoformat()}

    except Exception as e:
        print(f"[Celery][ERROR] Earnings Quality analysis failed: {str(e)}")
        return {"status": "error", "error": str(e)}
@app.task(name='aignitequant.tasks.run_follow_the_money_sector')
@skip_on_market_holiday
def run_follow_the_money_sector():
    """
    Execute Follow The Money sector rotation analysis
    
    This comprehensive analysis evaluates:
    - Sector relative strength and momentum
    - Institutional money flow patterns
    - Individual stock leaders within each sector
    - Market regime (bull/bear) and risk levels
    - Actionable trading recommendations
    
    Results are saved to reports/latest_sector_analysis.json
    Frontend fetches via /sector-analysis/latest endpoint
    
    Runs 3x per trading day:
    - 9:45 AM ET: After market open volatility settles
    - 12:30 PM ET: Midday update
    - 4:15 PM ET: After market close with full day's data
    """
    from datetime import datetime
    
    now = datetime.now()
    print(f"[Celery] Running Follow The Money (sector) analysis at {now}")
    
    try:
        # Import inside task to avoid circular imports
        from aignitequant.app.strategies.follow_the_money import main as run_analysis
        import asyncio
        
        # Run the async analysis
        asyncio.run(run_analysis())
        
        print(f"[Celery] Follow The Money (sector) analysis completed successfully")
        publish_update("follow_the_money_sector")
        return {"status": "success", "timestamp": now.isoformat()}
        
    except Exception as e:
        print(f"[Celery][ERROR] Follow The Money (sector) analysis failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_felix_strategy')
@skip_on_market_holiday
def run_felix_strategy():
    """Execute Felix Strategy (SMA crossover + volume spike detection)"""
    import asyncio
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Felix strategy at {now}")
    
    try:
        from aignitequant.app.strategies.felix_strategy import run_and_store_felix
        results = asyncio.run(run_and_store_felix())
        print(f"[Celery] Felix strategy completed successfully")
        publish_update("felix")
        return {"status": "success", "timestamp": now.isoformat()}
    except Exception as e:
        print(f"[Celery][ERROR] Felix strategy failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_vibia_hybrid')
@skip_on_market_holiday
def run_vibia_hybrid():
    """Execute Vibia J Hybrid Strategy (CANSLIM + TQQQ entry/exit)"""
    import asyncio
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Vibia Hybrid strategy at {now}")
    
    try:
        from aignitequant.app.strategies.vibia_j_hybrid_strategy import run_and_store_vibia_hybrid
        results = asyncio.run(run_and_store_vibia_hybrid())
        print(f"[Celery] Vibia Hybrid strategy completed successfully")
        publish_update("vibia_hybrid")
        return {"status": "success", "timestamp": now.isoformat()}
    except Exception as e:
        print(f"[Celery][ERROR] Vibia Hybrid strategy failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_marios_swing')
@skip_on_market_holiday
def run_marios_swing():
    """Execute Marios Stamatoudis Swing Trade Strategy"""
    import asyncio
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Marios Swing strategy at {now}")
    
    try:
        from aignitequant.app.strategies.marios_stamatoudis_swing_strategy import run_and_store_swing_trades
        results = asyncio.run(run_and_store_swing_trades())
        print(f"[Celery] Marios Swing strategy completed successfully")
        publish_update("marios_swing")
        return {"status": "success", "timestamp": now.isoformat()}
    except Exception as e:
        print(f"[Celery][ERROR] Marios Swing strategy failed: {str(e)}")
        return {"status": "error", "error": str(e)}
