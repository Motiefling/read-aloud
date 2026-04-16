"""Redis pub/sub helper to wake the dispatcher loop on the Celery worker."""

import logging

import redis

from app.config import settings

logger = logging.getLogger(__name__)


def notify_queue_changed():
    """Publish a signal to wake the dispatcher loop."""
    try:
        r = redis.Redis.from_url(settings.celery.broker_url)
        r.publish("queue:work_available", "1")
    except Exception as e:
        logger.warning("Failed to publish queue signal: %s", e)
