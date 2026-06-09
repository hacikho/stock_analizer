"""
Redis Pub/Sub event bus for real-time frontend updates.

When a Celery task writes new strategy results to the DB it calls
publish_update(strategy_name).  The FastAPI /events SSE endpoint
subscribes to the same channel and streams those notifications to
every connected browser tab — no polling required.

Channel layout
--------------
  strategy_updates   —  one channel, all strategies
  Message payload (JSON string):
    {"strategy": "<name>", "timestamp": "<ISO>"}

Strategy name tokens (match frontend route keys):
  market_pulse, canslim, bora, golden_cross, stage2, vcp,
  options, follow_the_money, follow_the_money_sector,
  earnings_quality, felix, vibia_hybrid, marios_swing
"""

import datetime
import json
import os

import redis

# Reuse the same Redis URL logic as market_pulse.py
_REDIS_URL = (
    os.getenv("REDIS_PRIVATE_URL") or
    os.getenv("REDIS_URL") or
    os.getenv("CELERY_BROKER_URL") or
    "redis://localhost:6379/0"
)

REDIS_CHANNEL = "strategy_updates"

_redis_client: "redis.Redis | None" = None


def _get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(_REDIS_URL, decode_responses=True)
    return _redis_client


def publish_update(strategy: str) -> None:
    """
    Publish a strategy-updated notification to Redis.
    Safe to call from Celery tasks (sync context).
    Errors are swallowed so a Redis blip never fails a strategy task.
    """
    try:
        payload = json.dumps({
            "strategy": strategy,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        })
        _get_redis().publish(REDIS_CHANNEL, payload)
        print(f"[events] Published update: {strategy}")
    except Exception as e:
        print(f"[events] WARNING: publish_update failed for {strategy}: {e}")
