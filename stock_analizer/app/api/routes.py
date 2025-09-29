from app.db import Stage2Data

from app.db import OptionSignalData

from app.strategies.leap_option_strategy1 import get_qqq_leap_signal
from app.strategies.leap_option_stategy2 import get_qqq_gap_down_leap_signal

from datetime import date
from sqlalchemy.orm import Session
from app.db import SessionLocal, CanSlimData, BoraData, GoldenCrossData


# app/api/routes.py

from fastapi import APIRouter, Query
import asyncio
from app.strategies.stage2 import check_trend_template
from app.services.sp500 import get_sp500_tickers, clear_sp500_cache
from app.strategies.bora_strategy import scan_symbols
from typing import Optional
from app.strategies.golden_cross_strategy import golden_cross_strategy
import aiohttp
from fastapi import HTTPException
from app.strategies.canslim_strategy import canslim_screen
from app.services.fear_greed import get_cnn_fear_greed



router = APIRouter()

# Get today's most recent CANSLIM data
@router.get("/canslim_db", tags=["CANSLIM"])
def get_canslim_db():
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

# Get latest results for all option strategies
@router.get("/options_signals", tags=["LEAP Option"])
def get_options_signals():
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


@router.get("/", tags=["Health Check"])
def root():
    return {
        "message": "🚀 Welcome to the Stock Analysis API",
        "endpoints": [
            "/stage_2 – Trend analysis strategy",
            "/bora – Bora Ozkent strategy",
            "/canslim – Canslim strategy",
            "/golden_cross – Golden Cross strategy",
            "/refresh_cache – Clear S&P 500 cache",
            "/fear-greed – CNN Fear & Greed Index"
        ],
        "status": "OK"
    }

@router.get("/stage_2")
async def trending_stocks():
    tickers = await get_sp500_tickers()
    tasks = [check_trend_template(t) for t in tickers]
    results = await asyncio.gather(*tasks)
    return {
        "qualified_stocks": [t for t, passed in zip(tickers, results) if passed]
    }

@router.get("/refresh_cache")
def refresh_cache():
    clear_sp500_cache()
    return {"message": "S&P 500 tickers cache cleared"}


@router.get("/bora")
async def bora_strategy(
    symbols: Optional[str] = Query(None, description="Comma-separated list of stock symbols. If not provided, S&P 500 will be used."),
    method: str = Query("slope", enum=["slope", "pct", "strict"]),
    slope_thresh: float = Query(0.0, description="Minimum slope (for 'slope' method)"),
    pct_thresh: float = Query(1.0, description="Minimum % increase (for 'pct' method)"),
    lookback: int = Query(10, description="Days to check EMA trend")
):
    if symbols:
        symbol_list = [s.strip().upper() for s in symbols.split(",")]
    else:
        symbol_list = await get_sp500_tickers()

    picks = await scan_symbols(
        symbol_list,
        ema21_method=method,
        slope_thresh=slope_thresh,
        pct_thresh=pct_thresh,
        lookback=lookback
    )
    return {"qualified_stocks": picks}



@router.get("/canslim")
async def canslim(
    symbols: Optional[str] = Query(
        None,
        description="Comma-separated tickers. If omitted, uses all S&P 500."
    )
):
    if symbols:
        tickers = [s.strip().upper() for s in symbols.split(",")]
    else:
        tickers = await get_sp500_tickers()

    results = await canslim_screen(tickers)
    return {"qualified_stocks": results}


@router.get("/golden_cross")
async def golden_cross():
    async with aiohttp.ClientSession() as session:
        picks = await golden_cross_strategy(session)
        return {"qualified_stocks": picks}


@router.get("/fear-greed")
def fear_greed_index():
    cnn = get_cnn_fear_greed()
    print("[DEBUG] CNN Fear & Greed Index:", cnn)

    if not cnn or not isinstance(cnn.get("cnn_fear_greed_score"), ( int, float )):
        return {"error": "Could not retrieve CNN index"}

    score = cnn["cnn_fear_greed_score"]
    if 0 <= score <= 100:
        return {
            "index": score,
            "comment": cnn["comment"]
        }

    return {"error": "CNN index out of range"}
