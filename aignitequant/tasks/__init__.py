"""
Celery tasks for scheduled strategy execution
"""
from .celery_app import app

__all__ = ['app']
