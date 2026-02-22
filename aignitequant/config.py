"""
Configuration management for AigniteQuant API
"""
import os
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # App Config
    APP_NAME: str = "AigniteQuant Stock Analysis API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    
    # API Config
    API_HOST: str = "localhost"
    API_PORT: int = 8000
    
    # Database Config
    DATABASE_URL: Optional[str] = None
    
    # Celery/Redis Config
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"
    
    # External APIs
    POLYGON_API_KEY: Optional[str] = None
    API_KEY: Optional[str] = None
    RAPIDAPI_KEY: Optional[str] = None
    FMP_API_KEY: Optional[str] = None
    
    # CORS Settings
    CORS_ORIGINS: list = [
        "http://localhost:5173",
        "http://localhost:3000",
        "http://localhost:5174",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
    ]
    
    # Paths
    REPORTS_DIR: str = "reports"
    DATA_DIR: str = "data"
    MODELS_DIR: str = "data/models"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Create global settings instance
settings = Settings()
