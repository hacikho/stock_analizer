"""
Celery application configuration
"""
from celery import Celery
from aignitequant.config import settings

# Configure Celery app
app = Celery(
    'aignitequant',
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=['aignitequant.tasks.strategy_tasks']
)

# Celery configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='US/Eastern',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
)

if __name__ == '__main__':
    app.start()
