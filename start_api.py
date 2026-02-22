#!/usr/bin/env python
"""
Start the AigniteQuant API server
"""
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import uvicorn
from aignitequant.config import settings

if __name__ == "__main__":
    # Railway sets PORT env var; fall back to API_PORT from config
    port = settings.PORT or settings.API_PORT
    host = settings.API_HOST
    
    print(f"🚀 Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    print(f"📍 Server: http://{host}:{port}")
    print(f"📚 API Docs: http://{host}:{port}/docs")
    
    uvicorn.run(
        "aignitequant.app.main:app",
        host=host,
        port=port,
        reload=settings.DEBUG
    )
