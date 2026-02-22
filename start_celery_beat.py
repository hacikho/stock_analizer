#!/usr/bin/env python
"""
Start Celery Beat scheduler for periodic tasks
Usage: python start_celery_beat.py
"""
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aignitequant.tasks.celery_app import app
# Import scheduler to register periodic tasks
from aignitequant.tasks import scheduler

if __name__ == '__main__':
    print("⏰ Starting Celery Beat scheduler...")
    print("📅 Scheduling tasks for market hours (9:30 AM - 4:00 PM ET, Mon-Fri)")
    
    # Start the beat scheduler
    app.Beat().run()
