"""
Celery tasks for executing trading strategies
"""
import datetime
import pytz
from aignitequant.tasks.celery_app import app
from aignitequant.app.db import SessionLocal, OptionSignalData, CanSlimData


EASTERN = pytz.timezone('US/Eastern')


# ============================================================
# Market Data Fetch Task — runs every 10 minutes during market hours
# ============================================================
@app.task(name='aignitequant.tasks.fetch_market_data', time_limit=600)
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


@app.task(name='aignitequant.tasks.run_option_strategies')
def run_option_strategies():
    """
    Execute option trading strategies and save results to database
    """
    from aignitequant.app.strategies.leap_option_strategy1 import get_qqq_leap_signal
    from aignitequant.app.strategies.leap_option_strategy2 import get_qqq_gap_down_leap_signal
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running option strategies at {now}")
    
    db = SessionLocal()
    try:
        # Run strategy 1
        result1 = get_qqq_leap_signal()
        if result1.get("signal") != "NONE":
            record = OptionSignalData(
                strategy_name="QQQ LEAP Strategy 1",
                signal_type=result1["signal"],
                entry_price=result1.get("entry_price"),
                strike_price=result1.get("strike_price"),
                expiration_date=result1.get("expiration_date"),
                timestamp=now
            )
            db.add(record)
        
        # Run strategy 2
        result2 = get_qqq_gap_down_leap_signal()
        if result2.get("signal") != "NONE":
            record = OptionSignalData(
                strategy_name="QQQ LEAP Strategy 2",
                signal_type=result2["signal"],
                entry_price=result2.get("entry_price"),
                strike_price=result2.get("strike_price"),
                expiration_date=result2.get("expiration_date"),
                timestamp=now
            )
            db.add(record)
        
        db.commit()
        print("[Celery] Option strategies completed successfully")
        return {"status": "success", "timestamp": now.isoformat()}
    
    except Exception as e:
        db.rollback()
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}
    finally:
        db.close()


@app.task(name='aignitequant.tasks.run_canslim')
def run_canslim():
    """
    Execute CANSLIM screening strategy
    """
    from aignitequant.app.strategies.canslim_strategy import canslim_screen
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running CANSLIM strategy at {now}")
    
    try:
        results = canslim_screen()
        print(f"[Celery] CANSLIM completed: found {len(results)} candidates")
        return {"status": "success", "candidates": len(results), "timestamp": now.isoformat()}
    
    except Exception as e:
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_bora_strategy')
def run_bora_strategy():
    """
    Execute BORA (Breakout Reversal Analysis) strategy
    """
    from aignitequant.app.strategies.bora_strategy import scan_symbols
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running BORA strategy at {now}")
    
    try:
        results = scan_symbols()
        print(f"[Celery] BORA completed: found {len(results)} signals")
        return {"status": "success", "signals": len(results), "timestamp": now.isoformat()}
    
    except Exception as e:
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_golden_cross')
def run_golden_cross():
    """
    Execute Golden Cross strategy
    """
    from aignitequant.app.strategies.golden_cross_strategy import golden_cross_strategy
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Golden Cross strategy at {now}")
    
    try:
        results = golden_cross_strategy()
        print(f"[Celery] Golden Cross completed: found {len(results)} signals")
        return {"status": "success", "signals": len(results), "timestamp": now.isoformat()}
    
    except Exception as e:
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_stage2')
def run_stage2():
    """
    Execute Stage 2 trend analysis
    """
    from aignitequant.app.strategies.stage2 import check_trend_template
    from aignitequant.app.services.sp500 import get_sp500_tickers
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Stage 2 analysis at {now}")
    
    try:
        tickers = get_sp500_tickers()
        # Process Stage 2 for all tickers
        results = []
        for ticker in tickers:
            result = check_trend_template(ticker)
            if result:
                results.append(result)
        
        print(f"[Celery] Stage 2 completed: found {len(results)} candidates")
        return {"status": "success", "candidates": len(results), "timestamp": now.isoformat()}
    
    except Exception as e:
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_vcp_scanner')
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
        return {"status": "success", "candidates": saved_count, "timestamp": now.isoformat()}
    
    except Exception as e:
        print(f"[Celery][ERROR] {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_follow_the_money')
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
        return {"status": "success", "timestamp": now.isoformat()}
    
    except Exception as e:
        print(f"[Celery][ERROR] Follow-The-Money failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_earnings_quality')
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
        return {"status": "success", "timestamp": now.isoformat()}
    
    except Exception as e:
        print(f"[Celery][ERROR] Earnings Quality analysis failed: {str(e)}")
        return {"status": "error", "error": str(e)}
@app.task(name='aignitequant.tasks.run_follow_the_money_sector')
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
        return {"status": "success", "timestamp": now.isoformat()}
        
    except Exception as e:
        print(f"[Celery][ERROR] Follow The Money (sector) analysis failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_felix_strategy')
def run_felix_strategy():
    """Execute Felix Strategy (SMA crossover + volume spike detection)"""
    import asyncio
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Felix strategy at {now}")
    
    try:
        from aignitequant.app.strategies.felix_strategy import run_and_store_felix
        results = asyncio.run(run_and_store_felix())
        print(f"[Celery] Felix strategy completed successfully")
        return {"status": "success", "timestamp": now.isoformat()}
    except Exception as e:
        print(f"[Celery][ERROR] Felix strategy failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_vibia_hybrid')
def run_vibia_hybrid():
    """Execute Vibia J Hybrid Strategy (CANSLIM + TQQQ entry/exit)"""
    import asyncio
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Vibia Hybrid strategy at {now}")
    
    try:
        from aignitequant.app.strategies.vibia_j_hybrid_strategy import run_and_store_vibia_hybrid
        results = asyncio.run(run_and_store_vibia_hybrid())
        print(f"[Celery] Vibia Hybrid strategy completed successfully")
        return {"status": "success", "timestamp": now.isoformat()}
    except Exception as e:
        print(f"[Celery][ERROR] Vibia Hybrid strategy failed: {str(e)}")
        return {"status": "error", "error": str(e)}


@app.task(name='aignitequant.tasks.run_marios_swing')
def run_marios_swing():
    """Execute Marios Stamatoudis Swing Trade Strategy"""
    import asyncio
    
    now = datetime.datetime.now(EASTERN)
    print(f"[Celery] Running Marios Swing strategy at {now}")
    
    try:
        from aignitequant.app.strategies.marios_stamatoudis_swing_strategy import run_and_store_swing_trades
        results = asyncio.run(run_and_store_swing_trades())
        print(f"[Celery] Marios Swing strategy completed successfully")
        return {"status": "success", "timestamp": now.isoformat()}
    except Exception as e:
        print(f"[Celery][ERROR] Marios Swing strategy failed: {str(e)}")
        return {"status": "error", "error": str(e)}