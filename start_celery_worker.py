#!/usr/bin/env python
"""
Start Celery worker for executing scheduled tasks
Usage: python start_celery_worker.py
"""
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aignitequant.tasks.celery_app import app

if __name__ == '__main__':
    print("🔄 Starting Celery worker...")
    print("📋 Tasks will be executed during market hours (9:30 AM - 4:00 PM ET)")
    
    # Start the worker
    app.worker_main([
        'worker',
        '--loglevel=info',
        '--pool=solo'  # Use solo pool for Windows compatibility
    ])
