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
# Import the scheduler so app.conf.beat_schedule is registered for the
# embedded Beat (-B) below. Without this import the embedded scheduler
# would start with an empty schedule.
from aignitequant.tasks import scheduler  # noqa: F401

if __name__ == '__main__':
    print("🔄 Starting Celery worker...")
    print("📋 Tasks will be executed during market hours (9:30 AM - 4:00 PM ET)")
    
    print("Celery Beat running embedded (-B); no separate beat service needed.")
    # Start the worker WITH the Beat scheduler embedded (-B), so the
    # standalone celery-beat service can be removed from Railway.
    app.worker_main([
        'worker',
        '--loglevel=info',
        '--pool=solo',  # Use solo pool for Windows compatibility
        '--beat',       # Embed Celery Beat -- replaces the separate beat service
    ])
