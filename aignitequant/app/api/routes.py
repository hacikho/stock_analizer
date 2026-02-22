from aignitequant.app.db import Stage2Data, OptionSignalData, SessionLocal, CanSlimData, BoraData, GoldenCrossData, VCPData, EarningsQualityData, FelixData, MarketData, MarketDataMeta
from aignitequant.app.strategies.leap_option_strategy1 import get_qqq_leap_signal
from aignitequant.app.strategies.leap_option_strategy2 import get_qqq_gap_down_leap_signal
from aignitequant.app.services.sp500 import clear_sp500_cache
from aignitequant.app.services.fear_greed import get_cnn_fear_greed

from datetime import date
from sqlalchemy.orm import Session
from fastapi import APIRouter, Query, HTTPException
from typing import Optional



router = APIRouter()

# Landing page / Health check
@router.get("/", tags=["General"])
async def root():
    """
    API Health Check and Endpoint Discovery
    
    Returns API status and available endpoints organized by category.
    Use this endpoint to verify API is online and discover available strategies.
    
    Returns:
        dict: API status, version, and endpoint directory
    """
    return {
        "status": "online",
        "app": "Stock Market Analysis API",
        "version": "1.0.0",
        "endpoints": {
            "docs": "/docs",
            "redoc": "/redoc",
            "strategies": {
                "canslim": "/canslim_db",
                "bora": "/bora_db",
                "golden_cross": "/golden_cross_db",
                "stage2": "/stage2_db",
                "vcp": "/vcp_db",
                "earnings_quality": "/earnings_quality_db",
                "felix": "/felix_db",
                "options": "/options_signals",
                "fear_greed": "/fear_greed"
            },
            "market_data": {
                "status": "/market_data/status",
                "ticker": "/market_data/ticker/{symbol}",
                "fetch": "/market_data/fetch (POST)"
            }
        }
    }

# Get today's most recent CANSLIM data
@router.get("/canslim_db", tags=["CANSLIM"])
def get_canslim_db():
    """
    Get CANSLIM Strategy Results from Database
    
    CANSLIM screens S&P 500 stocks using William O'Neil's methodology:
    - C: Current Quarterly Earnings
    - A: Annual Earnings Growth
    - N: New Product/Service/Management
    - S: Supply & Demand (shares outstanding)
    - L: Leader or Laggard
    - I: Institutional Sponsorship
    - M: Market Direction
    
    Data Source: Pre-calculated results from scheduled task
    Schedule: Runs every 30 minutes during market hours
    
    Returns:
        dict: Today's latest CANSLIM scan results with date, time, and qualified stocks
    """
    session: Session = SessionLocal()
    try:
        today = date.today()
        # Get the most recent time for today
        latest = session.query(CanSlimData.data_time).filter(CanSlimData.data_date == today).order_by(CanSlimData.data_time.desc()).first()
        if not latest:
            return {"error": "No CANSLIM data for today"}
        # Get all rows for today with the most recent time
        rows = session.query(CanSlimData).filter(CanSlimData.data_date == today, CanSlimData.data_time == latest[0]).all()
        result = [row.data_json for row in rows]
        return {"date": str(today), "time": str(latest[0]), "results": result}
    finally:
        session.close()

# Get today's most recent Bora strategy data
@router.get("/bora_db", tags=["BORA"])
def get_bora_db():
    """
    Get BORA Strategy Results from Database
    
    BORA (Bora Ozkent) strategy identifies stocks with strong upward momentum
    based on 21-day EMA (Exponential Moving Average) slope analysis.
    
    Criteria:
    - 21 EMA must be trending upward
    - Configurable slope/percentage thresholds
    - Lookback period for trend confirmation
    
    Data Source: Pre-calculated results from scheduled task
    Schedule: Runs every hour during market hours
    
    Returns:
        dict: Today's latest BORA scan results with date, time, and qualified stocks
    """
    session: Session = SessionLocal()
    try:
        today = date.today()
        latest = session.query(BoraData.data_time).filter(BoraData.data_date == today).order_by(BoraData.data_time.desc()).first()
        if not latest:
            return {"error": "No BORA data for today"}
        rows = session.query(BoraData).filter(BoraData.data_date == today, BoraData.data_time == latest[0]).all()
        result = [row.data_json for row in rows]
        return {"date": str(today), "time": str(latest[0]), "results": result}
    finally:
        session.close()


# Get today's most recent Golden Cross strategy data
@router.get("/golden_cross_db", tags=["GoldenCross"])
def get_golden_cross_db():
    """
    Get Golden Cross Strategy Results from Database
    
    Golden Cross is a bullish technical indicator that occurs when:
    - 50-day moving average crosses above 200-day moving average
    - Signals potential long-term uptrend
    - Classic buy signal for swing traders
    
    Data Source: Pre-calculated results from scheduled task
    Schedule: Runs once daily before market open (9:00 AM ET)
    
    Returns:
        dict: Today's Golden Cross opportunities with date, time, and qualified stocks
    """
    session: Session = SessionLocal()
    try:
        today = date.today()
        latest = session.query(GoldenCrossData.data_time).filter(GoldenCrossData.data_date == today).order_by(GoldenCrossData.data_time.desc()).first()
        if not latest:
            return {"error": "No Golden Cross data for today"}
        rows = session.query(GoldenCrossData).filter(GoldenCrossData.data_date == today, GoldenCrossData.data_time == latest[0]).all()
        result = [row.data_json for row in rows]
        return {"date": str(today), "time": str(latest[0]), "results": result}
    finally:
        session.close()

# Get today's most recent Stage 2 strategy data
@router.get("/stage2_db", tags=["Stage2"])
def get_stage2_db():
    """
    Get Stage 2 Trend Analysis Results from Database
    
    Stage 2 identifies stocks in the 'advancing' phase of Weinstein's market cycle:
    - Stage 1: Basing
    - Stage 2: Advancing (BUY ZONE)
    - Stage 3: Topping
    - Stage 4: Declining
    
    Criteria:
    - Price above rising 30-week moving average
    - Relative strength vs market
    - Volume confirmation
    
    Data Source: Pre-calculated results from scheduled task
    Schedule: Runs once daily after market close (4:30 PM ET)
    
    Returns:
        dict: Today's Stage 2 stocks with date, time, and qualified candidates
    """
    session: Session = SessionLocal()
    try:
        today = date.today()
        # Get the most recent time for today
        latest = session.query(Stage2Data.data_time).filter(Stage2Data.data_date == today).order_by(Stage2Data.data_time.desc()).first()
        if not latest:
            return {"error": "No Stage 2 data for today"}
        # Get all rows for today with the most recent time
        rows = session.query(Stage2Data).filter(Stage2Data.data_date == today, Stage2Data.data_time == latest[0]).all()
        result = [row.data_json for row in rows]
        return {"date": str(today), "time": str(latest[0]), "results": result}
    finally:
        session.close()


# Get today's most recent VCP (Volatility Contraction Pattern) scanner data
@router.get("/vcp_db", tags=["VCP"])
def get_vcp_db(limit: Optional[int] = Query(50, description="Maximum number of results to return")):
    """
    Get the most recent VCP scanner results
    
    Args:
        limit: Maximum number of VCP candidates to return (default: 50)
    
    Returns:
        VCP candidates with their analysis details
    """
    session: Session = SessionLocal()
    try:
        today = date.today()
        # Get the most recent time for today
        latest = session.query(VCPData.data_time).filter(VCPData.data_date == today).order_by(VCPData.data_time.desc()).first()
        
        if not latest:
            # If no data today, get the most recent data from any day
            latest_any_day = session.query(VCPData.data_date, VCPData.data_time).order_by(
                VCPData.data_date.desc(), VCPData.data_time.desc()
            ).first()
            
            if not latest_any_day:
                return {"error": "No VCP data available"}
            
            # Get all rows for the most recent date and time
            rows = session.query(VCPData).filter(
                VCPData.data_date == latest_any_day[0],
                VCPData.data_time == latest_any_day[1]
            ).limit(limit).all()
            
            result = [{
                "symbol": row.symbol,
                "sector": row.sector,
                "status": row.status,
                "details": row.data_json
            } for row in rows]
            
            return {
                "date": str(latest_any_day[0]),
                "time": str(latest_any_day[1]),
                "count": len(result),
                "results": result
            }
        
        # Get all rows for today with the most recent time
        rows = session.query(VCPData).filter(
            VCPData.data_date == today,
            VCPData.data_time == latest[0]
        ).limit(limit).all()
        
        result = [{
            "symbol": row.symbol,
            "sector": row.sector,
            "status": row.status,
            "details": row.data_json
        } for row in rows]
        
        return {
            "date": str(today),
            "time": str(latest[0]),
            "count": len(result),
            "results": result
        }
    finally:
        session.close()


# Get today's most recent Earnings Quality analysis data
@router.get("/earnings_quality_db", tags=["Earnings Quality"])
def get_earnings_quality_db():
    """
    Get the latest earnings quality analysis results from database.
    Returns stocks with recent earnings and their quality scores (0-100).
    
    Score ranges:
    - 80-100: BUY IMMEDIATELY (High quality earnings)
    - 60-79:  BUY IN 1-2 DAYS (Good earnings, minor concerns)
    - 40-59:  WAIT 3-5 DAYS (Mixed signals)
    - 20-39:  WAIT 1-2 WEEKS (Concerning signals)
    - 0-19:   AVOID (Poor earnings quality)
    """
    session: Session = SessionLocal()
    try:
        today = date.today()
        # Get the most recent time for today
        latest = session.query(EarningsQualityData.data_time).filter(
            EarningsQualityData.data_date == today
        ).order_by(EarningsQualityData.data_time.desc()).first()
        
        if not latest:
            return {"error": "No Earnings Quality data for today"}
        
        # Get all rows for today with the most recent time
        rows = session.query(EarningsQualityData).filter(
            EarningsQualityData.data_date == today,
            EarningsQualityData.data_time == latest[0]
        ).all()
        
        # Parse JSON data and return
        import json
        result = [json.loads(row.data_json) for row in rows]
        
        # Sort by total_score descending
        result.sort(key=lambda x: x.get('total_score', 0), reverse=True)
        
        return {
            "date": str(today),
            "time": str(latest[0]),
            "stocks_analyzed": len(result),
            "results": result
        }
    finally:
        session.close()


# Get today's most recent Felix Strategy data
@router.get("/felix_db", tags=["Felix"])
def get_felix_db():
    """
    Get Felix Strategy Results from Database

    Felix Strategy detects institutional-quality buying by scanning S&P 500 for:
    - Price crossing above the 50-day SMA in the last 3 trading days
    - 50-SMA curving upward (positive slope + acceleration)
    - Volume spike on crossover day (≥1.8× 50-day avg volume)

    Each result includes:
    - signal_quality (0-100 score)
    - institutional_strength (VERY STRONG / STRONG / MODERATE / WEAK)
    - volume_ratio, sma50_slope, sma50_acceleration

    Data Source: Pre-calculated results from scheduled task

    Returns:
        dict: Today's Felix scan results with date, time, and qualified stocks
    """
    session: Session = SessionLocal()
    try:
        today = date.today()
        latest = (
            session.query(FelixData.data_time)
            .filter(FelixData.data_date == today)
            .order_by(FelixData.data_time.desc())
            .first()
        )
        if not latest:
            # Fall back to the most recent data from any day
            latest_any = (
                session.query(FelixData.data_date, FelixData.data_time)
                .order_by(FelixData.data_date.desc(), FelixData.data_time.desc())
                .first()
            )
            if not latest_any:
                return {"error": "No Felix data available"}
            rows = (
                session.query(FelixData)
                .filter(FelixData.data_date == latest_any[0], FelixData.data_time == latest_any[1])
                .all()
            )
            import json as _json
            result = [_json.loads(row.data_json) for row in rows]
            result.sort(key=lambda x: x.get("signal_quality", 0), reverse=True)
            return {
                "date": str(latest_any[0]),
                "time": str(latest_any[1]),
                "count": len(result),
                "results": result,
            }

        rows = (
            session.query(FelixData)
            .filter(FelixData.data_date == today, FelixData.data_time == latest[0])
            .all()
        )
        import json as _json
        result = [_json.loads(row.data_json) for row in rows]
        result.sort(key=lambda x: x.get("signal_quality", 0), reverse=True)
        return {
            "date": str(today),
            "time": str(latest[0]),
            "count": len(result),
            "results": result,
        }
    finally:
        session.close()


# Get latest results for all option strategies
@router.get("/options_signals", tags=["LEAP Option"])
def get_options_signals():
    """
    Get Latest LEAP Option Signals from Database
    
    LEAP (Long-term Equity Anticipation Securities) options are long-dated options
    used for directional plays with lower capital requirements than stock ownership.
    
    This endpoint aggregates signals from multiple LEAP strategies:
    - leap_option_qqq: QQQ dip-buying opportunities (≥1% down + bull market)
    - leap_option_qqq_gap: QQQ gap-down plays (≥2% gap down)
    
    Returns:
        dict: Latest signals for each strategy with entry details
    """
    session = SessionLocal()
    try:
        # Get the latest entry for each strategy
        strategies = ["leap_option_qqq", "leap_option_qqq_gap"]  # Add more as you grow
        results = {}
        for strat in strategies:
            row = (
                session.query(OptionSignalData)
                .filter(OptionSignalData.strategy == strat)
                .order_by(OptionSignalData.data_date.desc(), OptionSignalData.data_time.desc())
                .first()
            )
            if row:
                results[strat] = row.data_json
            else:
                results[strat] = None
        return {"option_signals": results}
    finally:
        session.close()

# --- LEAP Option Strategy 1 Endpoint ---
@router.get("/leap_option_qqq", tags=["LEAP Option"])
def leap_option_qqq():
    """
    Returns a LEAP option signal for QQQ if QQQ is down >=1%, above 100SMA, and in a bull market.
    """
    signal = get_qqq_leap_signal()
    if signal:
        return {"signal": signal}
    return {"signal": None, "message": "No LEAP option signal for QQQ today."}

# --- LEAP Option Strategy 2 Endpoint ---
@router.get("/leap_option_qqq_gap", tags=["LEAP Option"])
def leap_option_qqq_gap():
    """
    Returns a LEAP option signal for QQQ if QQQ gaps down >=2% (open < -2% vs prev close).
    """
    signal = get_qqq_gap_down_leap_signal()
    if signal:
        return {"signal": signal}
    return {"signal": None, "message": "No LEAP option gap down signal for QQQ today."}


@router.get("/refresh_cache")
def refresh_cache():
    """
    Clear S&P 500 Tickers Cache
    
    Forces refresh of cached S&P 500 constituent list.
    Use this when:
    - New stocks added/removed from S&P 500
    - Cache appears stale or corrupted
    - Testing with fresh data
    
    Cache is auto-refreshed periodically, manual refresh rarely needed.
    
    Returns:
        dict: Confirmation message
    """
    clear_sp500_cache()
    return {"message": "S&P 500 tickers cache cleared"}


# ---------- Market Data Cache Endpoints ----------

@router.get("/market_data/status", tags=["Market Data"])
def market_data_status():
    """
    Check the status of the centralized market data cache.
    
    Returns:
        dict: Last fetch timestamp, total tickers/rows, freshness status
    """
    from aignitequant.app.services.market_data import is_market_data_fresh, get_last_fetch_time
    
    db = SessionLocal()
    try:
        # Count unique tickers and total rows
        ticker_count = db.query(MarketData.symbol).distinct().count()
        total_rows = db.query(MarketData).count()
        
        # Get metadata
        last_fetch = get_last_fetch_time()
        fresh = is_market_data_fresh(max_age_minutes=15)
        
        return {
            "status": "fresh" if fresh else "stale",
            "last_fetch_utc": last_fetch,
            "unique_tickers": ticker_count,
            "total_rows": total_rows,
            "is_fresh": fresh,
        }
    finally:
        db.close()


@router.get("/market_data/ticker/{symbol}", tags=["Market Data"])
def get_ticker_data(symbol: str, days: int = Query(default=30, ge=1, le=730)):
    """
    Retrieve cached OHLCV data for a specific ticker from the shared market_data table.
    
    Args:
        symbol: Stock ticker (e.g. AAPL)
        days: Number of calendar days of history (default: 30, max: 730)
    """
    from aignitequant.app.services.market_data import get_dataframe_from_db
    
    df = get_dataframe_from_db(symbol.upper(), days=days)
    if df is None or df.empty:
        raise HTTPException(status_code=404, detail=f"No market data found for {symbol.upper()}")
    
    records = df.reset_index().to_dict(orient="records")
    # Convert timestamps to strings for JSON serialization
    for r in records:
        r["timestamp"] = str(r["timestamp"].date()) if hasattr(r["timestamp"], "date") else str(r["timestamp"])
    
    return {
        "symbol": symbol.upper(),
        "count": len(records),
        "data": records,
    }


@router.post("/strategies/run_all", tags=["Strategies"])
async def run_all_strategies():
    """
    Manually trigger ALL strategy tasks.
    Requires market_data table to be populated first (POST /market_data/fetch).
    Each strategy reads from DB and writes results to its own table.
    """
    import traceback
    results = {}
    
    strategies = [
        ("canslim", "aignitequant.tasks.run_canslim"),
        ("bora", "aignitequant.tasks.run_bora_strategy"),
        ("golden_cross", "aignitequant.tasks.run_golden_cross"),
        ("stage2", "aignitequant.tasks.run_stage2"),
        ("vcp", "aignitequant.tasks.run_vcp_scanner"),
        ("felix", "aignitequant.tasks.run_felix_strategy"),
        ("vibia_hybrid", "aignitequant.tasks.run_vibia_hybrid"),
        ("marios_swing", "aignitequant.tasks.run_marios_swing"),
        ("follow_the_money", "aignitequant.tasks.run_follow_the_money"),
        ("earnings_quality", "aignitequant.tasks.run_earnings_quality"),
        ("options", "aignitequant.tasks.run_option_strategies"),
    ]
    
    from aignitequant.tasks.celery_app import app as celery_app
    
    for name, task_name in strategies:
        try:
            task = celery_app.send_task(task_name)
            results[name] = f"dispatched (task_id={task.id})"
        except Exception as e:
            results[name] = f"error: {e}"
    
    return {
        "status": "dispatched",
        "message": "All strategies sent to Celery worker. Check worker logs for progress.",
        "tasks": results
    }


@router.get("/market_data/test_fetch", tags=["Market Data"])
async def test_market_data_fetch():
    """
    Diagnostic endpoint: test S&P 500 scraping and Polygon API with 1 ticker.
    Use this to verify dependencies (lxml) and API key before running full fetch.
    """
    import traceback
    results = {}
    
    # Step 1: Test S&P 500 ticker fetch
    try:
        from aignitequant.app.services.sp500 import get_sp500_tickers
        tickers = await get_sp500_tickers()
        results["sp500_tickers"] = f"OK - {len(tickers)} tickers"
        results["sample_tickers"] = tickers[:5]
    except Exception as e:
        results["sp500_tickers"] = f"FAILED: {e}"
        results["sp500_traceback"] = traceback.format_exc()
        return results
    
    # Step 2: Test Polygon API with 1 ticker
    try:
        import os
        api_key = os.getenv("API_KEY")
        results["polygon_api_key"] = "SET" if api_key else "MISSING"
        
        if api_key:
            import aiohttp, datetime
            today = datetime.date.today()
            start = today - datetime.timedelta(days=30)
            url = f"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/{start}/{today}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params={"apiKey": api_key}) as resp:
                    results["polygon_status"] = resp.status
                    if resp.status == 200:
                        data = await resp.json()
                        results["polygon_results"] = len(data.get("results", []))
                    else:
                        results["polygon_body"] = await resp.text()
    except Exception as e:
        results["polygon_error"] = f"FAILED: {e}"
    
    return results


@router.post("/market_data/fetch", tags=["Market Data"])
async def trigger_market_data_fetch():
    """
    Manually trigger a market data fetch job.
    Useful for initial setup or after adding new tickers.
    This runs synchronously and may take 2-5 minutes for ~500 tickers.
    """
    import asyncio
    import traceback
    from aignitequant.app.services.market_data import fetch_all_market_data
    
    try:
        stats = await fetch_all_market_data(batch_size=5, delay=1.0)
        return {"status": "success", **stats}
    except Exception as e:
        print(f"❌ Market data fetch error: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/fear_greed")
def fear_greed_index():
    """
    Get CNN Fear & Greed Index
    
    Real-time market sentiment indicator (0-100 scale):
    - 0-24: Extreme Fear (potential buy opportunity)
    - 25-44: Fear
    - 45-55: Neutral
    - 56-75: Greed
    - 76-100: Extreme Greed (potential sell signal)
    
    Based on 7 market indicators:
    - Stock Price Momentum, Stock Price Strength, Stock Price Breadth
    - Put/Call Ratios, Market Volatility (VIX)
    - Safe Haven Demand, Junk Bond Demand
    
    Data Source: CNN Business (live)
    
    Returns:
        dict: Current fear/greed index (0-100) with sentiment comment
    """
    cnn = get_cnn_fear_greed()
    print("[DEBUG] CNN Fear & Greed Index:", cnn)

    if not cnn or not isinstance(cnn.get("cnn_fear_greed_score"), ( int, float )):
        return {"error": "Could not retrieve CNN index"}

    score = cnn["cnn_fear_greed_score"]
    if 0 <= score <= 100:
        return {
            "index": score,
            "comment": cnn["comment"],
            "last_update": cnn.get("last_update")
        }

    return {"error": "CNN index out of range"}

@router.get("/sector-analysis/latest", tags=["Sector Analysis"])
def get_latest_sector_analysis():
    """
    Get Complete Sector Rotation Analysis Report
    
    Comprehensive institutional-grade sector analysis including:
    - 11 sector rankings by relative strength & momentum
    - Individual stock leaders within each sector (top 5 per sector)
    - Market regime classification (BULL/BEAR)
    - Risk level assessment
    - Actionable buy/sell/watch lists
    - Investment themes and portfolio strategy
    
    Data Source: Pre-calculated from scheduled task
    Schedule: 3x per trading day (9:45 AM, 12:30 PM, 4:15 PM ET)
    File: reports/latest_sector_analysis.json
    
    Returns:
        dict: Full analysis report with all sections
    """
    try:
        import json
        import os
        
        latest_file = "reports/latest_sector_analysis.json"
        
        if not os.path.exists(latest_file):
            raise HTTPException(status_code=404, detail="No sector analysis report found")
        
        with open(latest_file, 'r') as f:
            report = json.load(f)
        
        return {
            "status": "success", 
            "data": report
        }
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="No analysis report found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load report: {str(e)}")

@router.get("/sector-analysis/summary", tags=["Sector Analysis"])
def get_sector_analysis_summary():
    """
    Get Condensed Sector Analysis Summary
    
    Lightweight version of sector analysis containing only essential info:
    - Market state and regime
    - Top 3 sectors only
    - Key insights (risk level, exposure recommendations)
    - Immediate buy/avoid lists
    - Primary investment theme
    
    Use this for:
    - Dashboard summary views
    - Quick market overview
    - Reduced payload size
    
    For full details, use /sector-analysis/latest instead.
    
    Returns:
        dict: Condensed summary with key metrics only
    """
    try:
        import json
        import os
        
        latest_file = "reports/latest_sector_analysis.json"
        
        if not os.path.exists(latest_file):
            raise HTTPException(status_code=404, detail="No sector analysis report found")
        
        with open(latest_file, 'r') as f:
            report = json.load(f)
        
        # Extract key summary information
        summary = {
            "timestamp": report["metadata"]["timestamp"],
            "market_state": report["market_overview"]["market_state"],
            "market_description": report["market_overview"]["market_description"],
            "top_sectors": report["sector_analysis"]["rankings"][:3],
            "key_insights": report["key_insights"],
            "immediate_buys": report["actionable_recommendations"]["immediate_buys"],
            "avoid_stocks": report["actionable_recommendations"]["avoid_stocks"],
            "primary_theme": report["investment_themes"]["primary_theme"]
        }
        
        return {
            "status": "success",
            "data": summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate summary: {str(e)}")

@router.get("/sector-analysis/trading-ideas", tags=["Sector Analysis"])
def get_trading_ideas():
    """
    Get Actionable Trading Recommendations
    
    Trading-focused view of sector analysis optimized for:
    - Stock picking
    - Portfolio construction
    - Position sizing decisions
    
    Includes:
    - Market regime & risk level
    - Recommended portfolio exposure (% allocation)
    - Buy candidates (immediate opportunities)
    - Avoid candidates (risk stocks)
    - Watch list (potential setups)
    - Portfolio strategy guidance
    - Top 3 sectors with their best stocks
    
    Use this for:
    - Trading dashboard
    - Daily stock watchlist generation
    - Position entry/exit decisions
    
    Returns:
        dict: Curated trading ideas with buy/sell/watch recommendations
    """
    try:
        import json
        import os
        
        latest_file = "reports/latest_sector_analysis.json"
        
        if not os.path.exists(latest_file):
            raise HTTPException(status_code=404, detail="No sector analysis report found")
        
        with open(latest_file, 'r') as f:
            report = json.load(f)
        
        trading_ideas = {
            "market_regime": report["market_overview"]["regime"],
            "risk_level": report["key_insights"]["risk_level"],
            "recommended_exposure": report["key_insights"]["recommended_exposure"],
            "buy_candidates": report["actionable_recommendations"]["immediate_buys"],
            "avoid_candidates": report["actionable_recommendations"]["avoid_stocks"],
            "watch_list": report["actionable_recommendations"]["watch_list"],
            "portfolio_strategy": report["actionable_recommendations"]["portfolio_strategy"],
            "top_sector_stocks": {
                sector["sector_name"]: sector["top_performers"]
                for sector in report["sector_analysis"]["rankings"][:3]
                if sector["top_performers"]
            }
        }
        
        return {
            "status": "success",
            "data": trading_ideas
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get trading ideas: {str(e)}")


# ============================================================
# INTRADAY DATA ENDPOINTS
# 10-minute bars for pre-market, regular, and after-hours sessions
# ============================================================

@router.get("/intraday/{symbol}", tags=["Intraday"])
def get_intraday(
    symbol: str,
    date: Optional[str] = Query(None, description="Date YYYY-MM-DD (defaults to today ET)"),
    session: Optional[str] = Query(None, description="Filter by session: pre, regular, post"),
):
    """
    Get 10-minute intraday bars for a ticker.
    
    Covers all extended-hours sessions:
    - pre     : 4:00 AM – 9:29 AM ET
    - regular : 9:30 AM – 3:59 PM ET
    - post    : 4:00 PM – 8:00 PM ET
    
    Returns bars indexed by Eastern Time.
    """
    import datetime as dt
    from aignitequant.app.services.intraday_data import get_intraday_from_db
    
    trade_date = None
    if date:
        try:
            trade_date = dt.datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    
    if session and session not in ("pre", "regular", "post"):
        raise HTTPException(status_code=400, detail="session must be 'pre', 'regular', or 'post'.")
    
    df = get_intraday_from_db(symbol.upper(), date=trade_date, session_filter=session)
    if df is None:
        return {"status": "no_data", "symbol": symbol.upper(), "bars": []}
    
    bars = []
    for ts, row in df.iterrows():
        bars.append({
            "timestamp_et": str(ts),
            "session": row["session"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "vwap": row.get("vwap"),
            "transactions": row.get("transactions"),
        })
    
    return {
        "status": "success",
        "symbol": symbol.upper(),
        "date": str(trade_date or "today"),
        "session_filter": session,
        "total_bars": len(bars),
        "bars": bars,
    }


@router.get("/intraday/{symbol}/summary", tags=["Intraday"])
def get_intraday_ticker_summary(
    symbol: str,
    date: Optional[str] = Query(None, description="Date YYYY-MM-DD (defaults to today ET)"),
):
    """
    Quick summary of intraday data for a ticker — bar counts per session,
    latest price, total volume, day high/low.
    """
    import datetime as dt
    from aignitequant.app.services.intraday_data import get_intraday_summary
    
    trade_date = None
    if date:
        try:
            trade_date = dt.datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    
    summary = get_intraday_summary(symbol.upper(), date=trade_date)
    if summary is None:
        return {"status": "no_data", "symbol": symbol.upper()}
    
    return {"status": "success", **summary}


@router.post("/intraday/fetch", tags=["Intraday"])
async def trigger_intraday_fetch():
    """
    Manually trigger an intraday data fetch for all S&P 500 tickers.
    Pulls 10-minute bars for today from Polygon.io.
    This runs synchronously and may take 2-5 minutes.
    """
    from aignitequant.app.services.intraday_data import fetch_intraday_data
    
    stats = await fetch_intraday_data(batch_size=5, delay=1.0)
    return {"status": "success", **stats}

