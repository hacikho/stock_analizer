from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Time, Text, DateTime, BigInteger, func, Index, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


# ============================================================
# Shared Market Data Table — centralized OHLCV price cache
# ============================================================
class MarketData(Base):
    """
    Centralized daily OHLCV price data for all tickers.
    
    Populated by a scheduled Celery task every 10 minutes during market hours.
    All strategies read from this table instead of calling the Polygon API directly,
    drastically reducing API calls and speeding up strategy execution.
    """
    __tablename__ = 'market_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    
    # Composite index for fast lookups: symbol + date (unique per day)
    __table_args__ = (
        Index('ix_market_data_symbol_date', 'symbol', 'trade_date', unique=True),
    )


class MarketDataMeta(Base):
    """
    Metadata tracking table for the market data fetch job.
    Stores the last successful fetch timestamp so strategies can check data freshness.
    """
    __tablename__ = 'market_data_meta'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String, nullable=False, unique=True)
    value = Column(Text, nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


# ============================================================
# Intraday Market Data — 10-minute OHLCV bars
# Covers pre-market (4:00 AM), regular (9:30 AM–4:00 PM), and
# after-hours (4:00–8:00 PM) sessions, all in Eastern Time.
# ============================================================
class IntradayBar(Base):
    """
    10-minute OHLCV bars for all tickers.
    
    Populated by a Celery task every 10 minutes during extended hours (4 AM – 8 PM ET).
    Stores bars for the current trading day; older bars are automatically pruned.
    
    Session values:
        - 'pre'     : Pre-market  (4:00 AM – 9:29 AM ET)
        - 'regular' : Regular     (9:30 AM – 3:59 PM ET)
        - 'post'    : After-hours (4:00 PM – 8:00 PM ET)
    """
    __tablename__ = 'intraday_bars'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False, index=True)
    bar_timestamp = Column(DateTime, nullable=False, index=True)   # UTC datetime of bar open
    bar_timestamp_et = Column(DateTime, nullable=False)             # Eastern Time equivalent
    session = Column(String(7), nullable=False, index=True)         # 'pre', 'regular', 'post'
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=True)
    vwap = Column(Float, nullable=True)
    transactions = Column(Integer, nullable=True)                   # trade count in the bar
    created_at = Column(DateTime, server_default=func.now())
    
    # Composite unique index: one bar per symbol per 10-min window
    __table_args__ = (
        Index('ix_intraday_symbol_ts', 'symbol', 'bar_timestamp', unique=True),
        Index('ix_intraday_session', 'session'),
    )


class CanSlimData(Base):
    __tablename__ = 'canslim_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=True)
    data_date = Column(Date, nullable=False)
    data_time = Column(Time, nullable=False)
    data_json = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class BoraData(Base):
    __tablename__ = 'bora_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=True)
    data_date = Column(Date, nullable=False)
    data_time = Column(Time, nullable=False)
    data_json = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class BoraPosition(Base):
    """Track active Bora strategy positions with entry data for exit monitoring"""
    __tablename__ = 'bora_positions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False, unique=True)
    entry_date = Column(Date, nullable=False)
    entry_price = Column(String, nullable=False)  # Store as string to avoid float issues
    entry_reason = Column(Text, nullable=True)
    stop_loss = Column(String, nullable=True)
    target_price = Column(String, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class GoldenCrossData(Base):
    __tablename__ = 'golden_cross_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=True)
    data_date = Column(Date, nullable=False)
    data_time = Column(Time, nullable=False)
    data_json = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())

# Table for storing all options strategy signals
class OptionSignalData(Base):
    __tablename__ = 'option_signal_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy = Column(String, nullable=False)  # e.g. 'leap_option_qqq', 'leap_option_qqq_gap', etc.
    symbol = Column(String, nullable=True)
    data_date = Column(Date, nullable=False)
    data_time = Column(Time, nullable=False)
    data_json = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
# Table for storing Mark Minervini Stage 2 strategy results
class Stage2Data(Base):
    __tablename__ = 'stage2_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    data_date = Column(Date, nullable=False)
    data_time = Column(Time, nullable=False)
    data_json = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


# Table for storing VCP (Volatility Contraction Pattern) scanner results
class VCPData(Base):
    __tablename__ = 'vcp_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    sector = Column(String, nullable=True)
    status = Column(String, nullable=True)  # VCP, Breakout, etc.
    data_date = Column(Date, nullable=False)
    data_time = Column(Time, nullable=False)
    data_json = Column(Text, nullable=False)  # Full result details
    created_at = Column(DateTime, server_default=func.now())


# Table for storing Earnings Quality Score analysis results
class EarningsQualityData(Base):
    __tablename__ = 'earnings_quality_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    earnings_date = Column(Date, nullable=False)  # When earnings were reported
    data_date = Column(Date, nullable=False)  # When analysis was run
    data_time = Column(Time, nullable=False)
    data_json = Column(Text, nullable=False)  # Full analysis details
    created_at = Column(DateTime, server_default=func.now())


# Table for storing swing trade strategy signals
class SwingTradeData(Base):
    __tablename__ = 'swing_trade_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy = Column(String, nullable=False)  # 'classic_breakout', 'episodic_pivot', 'parabolic_short'
    symbol = Column(String, nullable=False)
    data_date = Column(Date, nullable=False)
    data_time = Column(Time, nullable=False)
    data_json = Column(Text, nullable=False)  # Full signal details
    created_at = Column(DateTime, server_default=func.now())


# Table for storing Vibia J. hybrid strategy signals
class VibiaHybridData(Base):
    __tablename__ = 'vibia_hybrid_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy = Column(String, nullable=False)  # 'canslim_stock', 'tqqq_swing', 'market_assessment'
    symbol = Column(String, nullable=False)
    signal_type = Column(String, nullable=True)  # 'buy', 'sell', 'add_position', 'hold', 'assessment'
    data_date = Column(Date, nullable=False)
    data_time = Column(Time, nullable=False)
    data_json = Column(Text, nullable=False)  # Full signal details
    created_at = Column(DateTime, server_default=func.now())


# Table for storing Felix Strategy (Institutional 50-SMA Breakout) results
class FelixData(Base):
    __tablename__ = 'felix_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    data_date = Column(Date, nullable=False)
    data_time = Column(Time, nullable=False)
    data_json = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


# Always use project root for data.db
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_URL = f"sqlite:///{os.path.join(BASE_DIR, 'data.db')}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Enable WAL mode for concurrent reads during writes (critical for market data fetch + strategy reads)
@event.listens_for(engine, "connect")
def set_sqlite_wal_mode(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")  # Faster writes with WAL
    cursor.close()

# Create tables
Base.metadata.create_all(bind=engine)



