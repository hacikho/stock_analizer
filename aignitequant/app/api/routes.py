from aignitequant.app.db import (
    Stage2Data, OptionSignalData, SessionLocal, CanSlimData, BoraData,
    GoldenCrossData, VCPData, EarningsQualityData, FelixData, MarketData,
    MarketDataMeta, IntradayBar, BoraPosition, SwingTradeData, VibiaHybridData,
    FollowTheMoneyData,
)
from aignitequant.app.strategies.leap_option_strategy1 import get_qqq_leap_signal
from aignitequant.app.strategies.leap_option_strategy2 import get_qqq_gap_down_leap_signal
from aignitequant.app.services.sp500 import clear_sp500_cache
from aignitequant.app.services.fear_greed import get_cnn_fear_greed

from datetime import date
import asyncio
import datetime as _dt
import os
from sqlalchemy.orm import Session
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional


def format_last_updated(data_date, data_time) -> str:
    """
    Return a human-readable 'last updated' string, e.g. 'Jun 8, 2026 at 6:06 PM ET'.
    Accepts date/time as date/time objects or strings.
    """
    try:
        if isinstance(data_date, str):
            data_date = _dt.date.fromisoformat(data_date)
        if isinstance(data_time, str):
            data_time = _dt.time.fromisoformat(data_time.split(".")[0])
        dt = _dt.datetime.combine(data_date, data_time)
        return dt.strftime("%-d %b %Y at %-I:%M %p ET")
    except Exception:
        return str(data_date) if data_date else "Unknown"


router = APIRouter()


# ============================================================
# Server-Sent Events — real-time strategy update stream
# ============================================================

async def _sse_event_generator():
    """
    Subscribe to the Redis strategy_updates pub/sub channel and yield
    SSE-formatted messages to the connected browser client.

    Protocol:
      event: strategy_update
      data: {"strategy": "<name>", "timestamp": "<ISO>"}

    A comment ping is sent every 25 s so proxies/browsers don't drop
    an idle connection.
    """
    import redis.asyncio as aioredis
    from aignitequant.app.services.events import REDIS_CHANNEL

    redis_url = (
        os.getenv("REDIS_PRIVATE_URL") or
        os.getenv("REDIS_URL") or
        os.getenv("CELERY_BROKER_URL") or
        "redis://localhost:6379/0"
    )

    r = aioredis.from_url(redis_url, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe(REDIS_CHANNEL)

    try:
        # Confirm connection to the client
        yield ": connected\n\n"

        while True:
            try:
                message = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True),
                    timeout=25.0,
                )
                if message and message["type"] == "message":
                    yield f"event: strategy_update\ndata: {message['data']}\n\n"
                else:
                    # Keepalive ping — browsers/proxies drop idle SSE after ~30 s
                    yield ": ping\n\n"
            except asyncio.TimeoutError:
                yield ": ping\n\n"
    except asyncio.CancelledError:
        pass
    finally:
        await pubsub.unsubscribe(REDIS_CHANNEL)
        await r.aclose()


@router.get("/events", tags=["Events"])
async def events():
    """
    Server-Sent Events stream.  Connect once and receive a push notification
    every time a strategy task writes new results to the database.

    Event format:
        event: strategy_update
        data: {"strategy": "canslim", "timestamp": "2026-06-09T14:00:00Z"}

    Strategy names: market_pulse, canslim, bora, golden_cross, stage2, vcp,
    options, follow_the_money, follow_the_money_sector, earnings_quality,
    felix, vibia_hybrid, marios_swing

    Frontend usage (JavaScript):
        const es = new EventSource('/events');
        es.addEventListener('strategy_update', e => {
            const { strategy, timestamp } = JSON.parse(e.data);
            // re-fetch the endpoint for that strategy
        });
    """
    return StreamingResponse(
        _sse_event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering on Railway
            "Connection": "keep-alive",
        },
    )

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
                "vibia_hybrid": "/vibia_hybrid_db",
                "marios_swing": "/marios_swing_db",
                "options": "/options_signals",
                "fear_greed": "/fear_greed"
            },
            "market_data": {
                "status": "/market_data/status",
                "ticker": "/market_data/ticker/{symbol}",
                "fetch": "/market_data/fetch (POST)"
            },
            "market_pulse": {
                "snapshot": "/market-pulse"
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
            # Fall back to the most recent data from any day
            latest_any = session.query(CanSlimData.data_date, CanSlimData.data_time).order_by(
                CanSlimData.data_date.desc(), CanSlimData.data_time.desc()
            ).first()
            if not latest_any:
                return {"error": "No CANSLIM data available"}
            rows = session.query(CanSlimData).filter(
                CanSlimData.data_date == latest_any[0], CanSlimData.data_time == latest_any[1]
            ).all()
            result = [row.data_json for row in rows]
            return {"date": str(latest_any[0]), "time": str(latest_any[1]), "last_updated": format_last_updated(latest_any[0], latest_any[1]), "results": result}
        # Get all rows for today with the most recent time
        rows = session.query(CanSlimData).filter(CanSlimData.data_date == today, CanSlimData.data_time == latest[0]).all()
        result = [row.data_json for row in rows]
        return {"date": str(today), "time": str(latest[0]), "last_updated": format_last_updated(today, latest[0]), "results": result}
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
            # Fall back to the most recent data from any day
            latest_any = session.query(BoraData.data_date, BoraData.data_time).order_by(
                BoraData.data_date.desc(), BoraData.data_time.desc()
            ).first()
            if not latest_any:
                return {"error": "No BORA data available"}
            rows = session.query(BoraData).filter(
                BoraData.data_date == latest_any[0], BoraData.data_time == latest_any[1]
            ).all()
            result = [row.data_json for row in rows]
            return {"date": str(latest_any[0]), "time": str(latest_any[1]), "last_updated": format_last_updated(latest_any[0], latest_any[1]), "results": result}
        rows = session.query(BoraData).filter(BoraData.data_date == today, BoraData.data_time == latest[0]).all()
        result = [row.data_json for row in rows]
        return {"date": str(today), "time": str(latest[0]), "last_updated": format_last_updated(today, latest[0]), "results": result}
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
            # Fall back to the most recent data from any day
            latest_any = session.query(GoldenCrossData.data_date, GoldenCrossData.data_time).order_by(
                GoldenCrossData.data_date.desc(), GoldenCrossData.data_time.desc()
            ).first()
            if not latest_any:
                return {"error": "No Golden Cross data available"}
            rows = session.query(GoldenCrossData).filter(
                GoldenCrossData.data_date == latest_any[0], GoldenCrossData.data_time == latest_any[1]
            ).all()
            result = [row.data_json for row in rows]
            return {"date": str(latest_any[0]), "time": str(latest_any[1]), "last_updated": format_last_updated(latest_any[0], latest_any[1]), "results": result}
        rows = session.query(GoldenCrossData).filter(GoldenCrossData.data_date == today, GoldenCrossData.data_time == latest[0]).all()
        result = [row.data_json for row in rows]
        return {"date": str(today), "time": str(latest[0]), "last_updated": format_last_updated(today, latest[0]), "results": result}
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
            # Fall back to the most recent data from any day
            latest_any = session.query(Stage2Data.data_date, Stage2Data.data_time).order_by(
                Stage2Data.data_date.desc(), Stage2Data.data_time.desc()
            ).first()
            if not latest_any:
                return {"error": "No Stage 2 data available"}
            rows = session.query(Stage2Data).filter(
                Stage2Data.data_date == latest_any[0], Stage2Data.data_time == latest_any[1]
            ).all()
            result = [row.data_json for row in rows]
            return {"date": str(latest_any[0]), "time": str(latest_any[1]), "last_updated": format_last_updated(latest_any[0], latest_any[1]), "results": result}
        # Get all rows for today with the most recent time
        rows = session.query(Stage2Data).filter(Stage2Data.data_date == today, Stage2Data.data_time == latest[0]).all()
        result = [row.data_json for row in rows]
        return {"date": str(today), "time": str(latest[0]), "last_updated": format_last_updated(today, latest[0]), "results": result}
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
                "last_updated": format_last_updated(latest_any_day[0], latest_any_day[1]),
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
            "last_updated": format_last_updated(today, latest[0]),
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
            # Fall back to the most recent data from any day
            latest_any = session.query(EarningsQualityData.data_date, EarningsQualityData.data_time).order_by(
                EarningsQualityData.data_date.desc(), EarningsQualityData.data_time.desc()
            ).first()
            if not latest_any:
                return {"error": "No Earnings Quality data available"}
            latest = (latest_any[1],)
            today = latest_any[0]
        
        # Get all rows for the date with the most recent time
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
            "last_updated": format_last_updated(today, latest[0]),
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
                "last_updated": format_last_updated(latest_any[0], latest_any[1]),
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
            "last_updated": format_last_updated(today, latest[0]),
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


@router.get("/vibia_hybrid_db", tags=["Vibia Hybrid"])
def get_vibia_hybrid_db():
    """
    Get Vibia J. Hybrid Strategy Results from Database

    Vibia J. is a top performer in the US Investing Championship. Her hybrid system
    combines two complementary approaches:

    1. **CANSLIM Individual Stocks** — screens for market leaders with 25%+ quarterly
       earnings/sales growth, EPS+RS ratings ≥95, forming proper bases in Stage 1/2.
       Position sizing: 10% initial, up to 15% max, 6-8 stocks total.

    2. **TQQQ Swing Trading** — when individual setups are scarce or the market is choppy,
       pivots to TQQQ. Entry: Nasdaq pulls back 5-8%, then wait for 3 consecutive up days
       near the 21 EMA. Exit: partial trim when 10-15%+ extended above 21 EMA with downside
       reversal, or 2 closes below 21 EMA (check day 3 before full exit).

    Response structure:
    - `market_assessment`: current recommendation (individual_stocks / tqqq_or_cash /
      tqqq_early_entry / flexible) with distribution day count and market stage
    - `canslim_stocks`: list of CANSLIM buy signals sorted by 90-day return
    - `tqqq_entry`: TQQQ buy signal if 3 up days off pullback (or null)
    - `tqqq_exit`: TQQQ sell/trim signal if extension or 21 EMA break (or null)

    Data Source: Pre-calculated by scheduled Celery task every 15 min during market hours.
    """
    import json as _json
    session: Session = SessionLocal()
    try:
        # Find most recent batch regardless of date (fallback pattern)
        latest = (
            session.query(VibiaHybridData.data_date, VibiaHybridData.data_time)
            .order_by(VibiaHybridData.data_date.desc(), VibiaHybridData.data_time.desc())
            .first()
        )
        if not latest:
            return {"error": "No Vibia Hybrid data available"}

        data_date, data_time = latest

        rows = (
            session.query(VibiaHybridData)
            .filter(
                VibiaHybridData.data_date == data_date,
                VibiaHybridData.data_time == data_time,
            )
            .all()
        )

        canslim_stocks = []
        tqqq_entry = None
        tqqq_exit = None
        market_assessment = None

        for row in rows:
            data = _json.loads(row.data_json)
            if row.strategy == "canslim_stock":
                canslim_stocks.append(data)
            elif row.strategy == "tqqq_swing" and row.signal_type == "buy":
                tqqq_entry = data
            elif row.strategy == "tqqq_swing" and row.signal_type == "sell":
                tqqq_exit = data
            elif row.strategy == "market_assessment":
                market_assessment = data

        # Sort CANSLIM signals by 90-day return (strongest leaders first)
        canslim_stocks.sort(key=lambda x: x.get("returns_90d", 0), reverse=True)

        return {
            "date": str(data_date),
            "time": str(data_time),
            "last_updated": format_last_updated(data_date, data_time),
            "market_assessment": market_assessment,
            "canslim_stocks": canslim_stocks,
            "canslim_count": len(canslim_stocks),
            "tqqq_entry": tqqq_entry,
            "tqqq_exit": tqqq_exit,
        }
    finally:
        session.close()


@router.get("/marios_swing_db", tags=["Marios Swing"])
def get_marios_swing_db():
    """
    Get Marios Stamatoudis Swing Trading Strategy Results from Database

    Marios Stamatoudis is the 2023 US Investing Champion (291% return). His strategy
    covers three distinct swing setups, each with different risk profiles:

    1. **Classic Breakout** — stock makes a 30-100% prior move, then consolidates
       2 weeks–2 months with higher lows and a tightening range (<15%). Entry on
       trendline breakout; stop at breakout day's low; first target 2.75× ADR.
       Signals include `consolidation_lows_slope` and `rs_vs_spy_63d` (excess return
       vs SPY over 63 days — must be positive to pass the filter).

    2. **Episodic Pivot** — "sleepy stock" that was down 20%+ gaps up ≥5% on a
       fundamental catalyst with relative volume ≥1.5×. Entry at opening range high;
       stop at day's low. Note: `gap_detected: true` does NOT confirm a catalyst —
       always verify manually (see `catalyst_note`).

    3. **Parabolic Short** ⚠️ — stock made 100-400% parabolic move, RSI rolling over
       from overbought, close in lower third of day's range. **DAILY-BAR APPROXIMATION
       ONLY** — Marios' real entry triggers are intraday (ORB break / VWAP failure).
       Use these as watchlist candidates; confirm with `/intraday/{symbol}` before trading.

    Signals are sorted by risk/reward ratio descending within each category.

    Data Source: Pre-calculated by scheduled Celery task every 15 min during market hours.
    """
    import json as _json
    session: Session = SessionLocal()
    try:
        latest = (
            session.query(SwingTradeData.data_date, SwingTradeData.data_time)
            .order_by(SwingTradeData.data_date.desc(), SwingTradeData.data_time.desc())
            .first()
        )
        if not latest:
            return {"error": "No Marios Swing data available"}

        data_date, data_time = latest

        rows = (
            session.query(SwingTradeData)
            .filter(
                SwingTradeData.data_date == data_date,
                SwingTradeData.data_time == data_time,
            )
            .all()
        )

        classic_breakouts = []
        episodic_pivots = []
        parabolic_shorts = []

        for row in rows:
            data = _json.loads(row.data_json)
            if row.strategy == "classic_breakout":
                classic_breakouts.append(data)
            elif row.strategy == "episodic_pivot":
                episodic_pivots.append(data)
            elif row.strategy == "parabolic_short":
                parabolic_shorts.append(data)

        # Sort each category by risk/reward descending
        for lst in [classic_breakouts, episodic_pivots, parabolic_shorts]:
            lst.sort(key=lambda x: x.get("risk_reward", 0), reverse=True)

        total = len(classic_breakouts) + len(episodic_pivots) + len(parabolic_shorts)

        return {
            "date": str(data_date),
            "time": str(data_time),
            "last_updated": format_last_updated(data_date, data_time),
            "total_signals": total,
            "classic_breakouts": classic_breakouts,
            "classic_breakouts_count": len(classic_breakouts),
            "episodic_pivots": episodic_pivots,
            "episodic_pivots_count": len(episodic_pivots),
            "parabolic_shorts": parabolic_shorts,
            "parabolic_shorts_count": len(parabolic_shorts),
            "parabolic_short_warning": (
                "Parabolic short signals are daily-bar approximations only. "
                "Marios' actual entry triggers are intraday (ORB break / VWAP failure). "
                "Confirm with /intraday/{symbol} before placing any short position."
            ),
        }
    finally:
        session.close()


@router.post("/strategies/run_all", tags=["Strategies"])
async def run_all_strategies():
    """
    Manually trigger ALL strategies directly in the API process.
    Requires market_data table to be populated first (POST /market_data/fetch).
    Each strategy reads from DB and writes results to its own table.
    
    Note: Runs in-process for synchronous results. With PostgreSQL, Celery tasks
    also share the same DB and work correctly via celery-beat/worker.
    This may take several minutes to complete.
    """
    import traceback
    import asyncio
    results = {}
    
    # --- CANSLIM ---
    try:
        from aignitequant.app.strategies.canslim_strategy import run_and_store_canslim
        await run_and_store_canslim()
        results["canslim"] = "OK"
    except Exception as e:
        results["canslim"] = f"error: {e}"
    
    # --- BORA ---
    try:
        from aignitequant.app.strategies.bora_strategy import run_and_store_bora
        count = await run_and_store_bora()
        results["bora"] = f"OK - {count} picks"
    except Exception as e:
        results["bora"] = f"error: {e}"
    
    # --- Golden Cross ---
    try:
        from aignitequant.app.strategies.golden_cross_strategy import run_and_store_golden_cross
        await run_and_store_golden_cross()
        results["golden_cross"] = "OK"
    except Exception as e:
        results["golden_cross"] = f"error: {e}"
    
    # --- Stage 2 ---
    try:
        from aignitequant.app.strategies.stage2 import check_trend_template, get_spy_data
        from aignitequant.app.services.sp500 import get_sp500_tickers
        import json as _json
        
        tickers = await get_sp500_tickers()
        import aiohttp as _aiohttp
        async with _aiohttp.ClientSession() as _session:
            spy_data = await get_spy_data(_session)
        
        qualified = []
        now_dt = __import__('datetime').datetime.now()
        today_dt = now_dt.date()
        time_now = now_dt.time().replace(microsecond=0)
        
        for ticker in tickers:
            if await check_trend_template(ticker, spy_data):
                qualified.append(ticker)
                from aignitequant.app.db import Stage2Data
                entry = Stage2Data(
                    symbol=ticker,
                    data_date=today_dt,
                    data_time=time_now,
                    data_json=_json.dumps({"symbol": ticker, "date": str(today_dt), "criteria": "Stage2_with_RelativeStrength"})
                )
                db_session = SessionLocal()
                db_session.add(entry)
                db_session.commit()
                db_session.close()
        
        results["stage2"] = f"OK - {len(qualified)} candidates"
    except Exception as e:
        results["stage2"] = f"error: {e}"
    
    # --- VCP Scanner ---
    try:
        from aignitequant.app.strategies.vcp_scanner_strategy import scan_sp500_for_vcp, save_vcp_results_to_db
        vcp_results = await scan_sp500_for_vcp(batch_size=10, delay=2.0)
        saved = save_vcp_results_to_db(vcp_results)
        results["vcp"] = f"OK - {saved} candidates"
    except Exception as e:
        results["vcp"] = f"error: {e}"
    
    # --- Felix ---
    try:
        from aignitequant.app.strategies.felix_strategy import run_and_store_felix
        await run_and_store_felix()
        results["felix"] = "OK"
    except Exception as e:
        results["felix"] = f"error: {e}"
    
    # --- Vibia Hybrid ---
    try:
        from aignitequant.app.strategies.vibia_j_hybrid_strategy import run_and_store_vibia_hybrid
        await run_and_store_vibia_hybrid()
        results["vibia_hybrid"] = "OK"
    except Exception as e:
        results["vibia_hybrid"] = f"error: {e}"
    
    # --- Marios Swing ---
    try:
        from aignitequant.app.strategies.marios_stamatoudis_swing_strategy import run_and_store_swing_trades
        await run_and_store_swing_trades()
        results["marios_swing"] = "OK"
    except Exception as e:
        results["marios_swing"] = f"error: {e}"
    
    # --- Follow The Money ---
    try:
        from aignitequant.app.strategies.follow_the_money import main as run_ftm
        await run_ftm()
        results["follow_the_money"] = "OK"
    except Exception as e:
        results["follow_the_money"] = f"error: {e}"
    
    # --- Earnings Quality ---
    try:
        from aignitequant.app.strategies.earnings_quality_score import main as run_eq
        await run_eq()
        results["earnings_quality"] = "OK"
    except Exception as e:
        results["earnings_quality"] = f"error: {e}"
    
    # --- Options ---
    try:
        from aignitequant.app.strategies.leap_option_strategy1 import get_qqq_leap_signal
        from aignitequant.app.strategies.leap_option_strategy2 import get_qqq_gap_down_leap_signal
        import json as _ojson
        from datetime import datetime as _dt
        
        now = _dt.now()
        today_dt = now.date()
        time_now = now.time().replace(microsecond=0)
        
        r1 = get_qqq_leap_signal()
        r2 = get_qqq_gap_down_leap_signal()
        
        sig1 = r1.get('signal', 'NONE') if isinstance(r1, dict) else 'NONE'
        sig2 = r2.get('signal', 'NONE') if isinstance(r2, dict) else 'NONE'
        
        # Always save to option_signal_data table (even if no signal)
        db_session = SessionLocal()
        try:
            entry1 = OptionSignalData(
                strategy="leap_option_qqq",
                symbol="QQQ",
                data_date=today_dt,
                data_time=time_now,
                data_json=_ojson.dumps(r1 if isinstance(r1, dict) else {"signal": "NONE"}),
            )
            db_session.add(entry1)
            entry2 = OptionSignalData(
                strategy="leap_option_qqq_gap",
                symbol="QQQ",
                data_date=today_dt,
                data_time=time_now,
                data_json=_ojson.dumps(r2 if isinstance(r2, dict) else {"signal": "NONE"}),
            )
            db_session.add(entry2)
            db_session.commit()
        finally:
            db_session.close()
        
        results["options"] = f"OK - leap1:{sig1}, leap2:{sig2}"
    except Exception as e:
        results["options"] = f"error: {e}"
    
    return {
        "status": "completed",
        "message": "All strategies executed in-process. Results written to DB.",
        "tasks": results
    }


@router.get("/db/health", tags=["Diagnostics"])
def db_table_health():
    """
    Database Health Check — shows row count and latest write time for every table.

    Use this to verify:
    - PostgreSQL connection is working
    - Market data fetch is populating data
    - Each Celery scheduled task is running and writing results
    - How fresh the data is in each table

    Returns a status per table: row_count, latest_date, latest_time, last_created_at.
    """
    from sqlalchemy import func as sa_func

    session = SessionLocal()
    try:
        tables = {}

        # --- market_data (centralized OHLCV) ---
        count = session.query(sa_func.count(MarketData.id)).scalar() or 0
        latest = session.query(
            sa_func.max(MarketData.trade_date),
            sa_func.max(MarketData.created_at),
        ).first()
        tables["market_data"] = {
            "rows": count,
            "latest_trade_date": str(latest[0]) if latest and latest[0] else None,
            "last_write": str(latest[1]) if latest and latest[1] else None,
        }

        # --- market_data_meta ---
        count = session.query(sa_func.count(MarketDataMeta.id)).scalar() or 0
        latest = session.query(sa_func.max(MarketDataMeta.updated_at)).scalar()
        tables["market_data_meta"] = {
            "rows": count,
            "last_write": str(latest) if latest else None,
        }

        # --- intraday_bars ---
        count = session.query(sa_func.count(IntradayBar.id)).scalar() or 0
        latest = session.query(
            sa_func.max(IntradayBar.bar_timestamp),
            sa_func.max(IntradayBar.created_at),
        ).first()
        tables["intraday_bars"] = {
            "rows": count,
            "latest_bar": str(latest[0]) if latest and latest[0] else None,
            "last_write": str(latest[1]) if latest and latest[1] else None,
        }

        # --- Strategy tables (all share data_date / data_time / created_at) ---
        strategy_models = {
            "canslim_data": CanSlimData,
            "bora_data": BoraData,
            "golden_cross_data": GoldenCrossData,
            "stage2_data": Stage2Data,
            "vcp_data": VCPData,
            "felix_data": FelixData,
            "earnings_quality_data": EarningsQualityData,
            "option_signal_data": OptionSignalData,
            "swing_trade_data": SwingTradeData,
            "vibia_hybrid_data": VibiaHybridData,
        }
        for table_name, model in strategy_models.items():
            count = session.query(sa_func.count(model.id)).scalar() or 0
            latest = session.query(
                sa_func.max(model.data_date),
                sa_func.max(model.data_time),
                sa_func.max(model.created_at),
            ).first()
            tables[table_name] = {
                "rows": count,
                "latest_date": str(latest[0]) if latest and latest[0] else None,
                "latest_time": str(latest[1]) if latest and latest[1] else None,
                "last_write": str(latest[2]) if latest and latest[2] else None,
            }

        # --- bora_positions (separate structure) ---
        count = session.query(sa_func.count(BoraPosition.id)).scalar() or 0
        latest = session.query(
            sa_func.max(BoraPosition.entry_date),
            sa_func.max(BoraPosition.created_at),
        ).first()
        tables["bora_positions"] = {
            "rows": count,
            "latest_entry_date": str(latest[0]) if latest and latest[0] else None,
            "last_write": str(latest[1]) if latest and latest[1] else None,
        }

        # Summary
        empty = [t for t, info in tables.items() if info["rows"] == 0]
        populated = [t for t, info in tables.items() if info["rows"] > 0]

        return {
            "status": "ok",
            "summary": {
                "total_tables": len(tables),
                "populated": len(populated),
                "empty": len(empty),
                "empty_tables": empty,
            },
            "tables": tables,
        }
    except Exception as e:
        import traceback
        return {"status": "error", "error": str(e), "traceback": traceback.format_exc()}
    finally:
        session.close()


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

@router.get("/market-pulse", tags=["Market Pulse"])
def market_pulse():
    """
    Live market snapshot for 8 macro instruments.

    Returns the latest price, OHLCV, daily change, and change % for:
      - S&P 500 (via I:SPX index — Yahoo ^GSPC)
      - Nasdaq Composite (via I:COMP index — Yahoo ^IXIC)
      - Dow 30 (via I:DJI index — Yahoo ^DJI)
      - Russell 2000 (via I:RUT index — Yahoo ^RUT)
      - VIX / Volatility (via I:VIX — CBOE Volatility Index, spot, matches Yahoo ^VIX)
      - Gold (via GLD ETF proxy — Yahoo quotes GC=F futures)
      - Bitcoin (via X:BTCUSD — Polygon crypto)
      - Crude Oil (via USO ETF proxy — Yahoo quotes CL=F futures)

    Data is refreshed every minute by the fetch_market_pulse Celery task
    and cached in Redis (TTL 5 min).  Reads are sub-millisecond — safe to
    poll every 10-30 seconds from the frontend.

    Note: the five equity indices (I:*) require the Polygon Indices add-on.
    Gold and Crude Oil remain ETF proxies; swap to Polygon Futures
    front-month tickers if the Futures add-on is enabled.

    Returns:
        dict: {
            "data": [...],          # List of instrument snapshots
            "last_updated": "...",  # UTC ISO timestamp of most recent fetch
            "count": 8,
            "stale": true           # Only present when Redis cache is empty
        }
    """
    from aignitequant.app.services.market_pulse import get_market_pulse

    return get_market_pulse()


@router.get("/market-pulse/debug", tags=["Market Pulse"])
def market_pulse_debug():
    """
    Debug endpoint: checks Redis connectivity and Polygon API key status.
    Use this to diagnose why /market-pulse returns stale:true in production.
    """
    import os
    import redis as redis_lib

    # Determine which Redis URL is being used
    redis_url = (
        os.getenv("REDIS_PRIVATE_URL") or
        os.getenv("REDIS_URL") or
        os.getenv("CELERY_BROKER_URL") or
        "redis://localhost:6379/0"
    )
    redis_host = redis_url.split("@")[-1] if "@" in redis_url else redis_url

    # Test Redis connectivity
    redis_ok = False
    redis_error = None
    cached_data = None
    try:
        r = redis_lib.from_url(redis_url, decode_responses=True, socket_timeout=3)
        r.ping()
        redis_ok = True
        cached_data = r.get("market_pulse:snapshot")
    except Exception as e:
        redis_error = str(e)

    # Check API key
    api_key = os.getenv("API_KEY")
    api_key_set = bool(api_key and len(api_key) > 10)

    return {
        "redis": {
            "url_used": redis_host,
            "connected": redis_ok,
            "error": redis_error,
            "has_cached_data": cached_data is not None,
        },
        "polygon": {
            "api_key_set": api_key_set,
            "api_key_preview": (api_key[:6] + "...") if api_key_set else None,
        },
        "env_vars_present": {
            "REDIS_PRIVATE_URL": bool(os.getenv("REDIS_PRIVATE_URL")),
            "REDIS_URL": bool(os.getenv("REDIS_URL")),
            "CELERY_BROKER_URL": bool(os.getenv("CELERY_BROKER_URL")),
            "API_KEY": api_key_set,
        },
    }


@router.post("/market-pulse/refresh", tags=["Market Pulse"])
async def market_pulse_refresh():
    """
    Manually trigger a market pulse fetch and cache to Redis.
    Returns the fetch stats plus the resulting cached data (or error detail).
    Useful for diagnosing why the cache is empty after deploy.
    """
    import traceback
    try:
        from aignitequant.app.services.market_pulse import fetch_and_store_market_pulse, get_market_pulse
        stats = await fetch_and_store_market_pulse()
        snapshot = get_market_pulse()
        return {"status": "ok", "stats": stats, "snapshot_count": snapshot.get("count"), "stale": snapshot.get("stale", False)}
    except Exception as e:
        return {"status": "error", "error": str(e), "traceback": traceback.format_exc()}


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

@router.get("/follow-the-money/latest", tags=["Follow The Money"])
def get_follow_the_money_latest():
    """
    Get the most recent Follow The Money sector rotation analysis from the database.
    Runs every 30 minutes during market hours (4 AM – 8 PM ET, Mon–Fri).
    """
    import json
    session = SessionLocal()
    try:
        row = (
            session.query(FollowTheMoneyData)
            .order_by(FollowTheMoneyData.data_date.desc(), FollowTheMoneyData.data_time.desc())
            .first()
        )
        if not row:
            raise HTTPException(status_code=404, detail="No Follow The Money data available yet")
        data = json.loads(row.data_json)
        data["last_updated"] = f"{row.data_date} {str(row.data_time)[:5]} ET"
        return {"status": "success", "data": data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()


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

