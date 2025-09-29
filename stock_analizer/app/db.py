from sqlalchemy import create_engine, Column, Integer, String, Date, Time, Text, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

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

# Always use project root for data.db
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_URL = f"sqlite:///{os.path.join(BASE_DIR, 'data.db')}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create tables
Base.metadata.create_all(bind=engine)



