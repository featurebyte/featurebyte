"""
Celery worker
"""
from celery import Celery

from featurebyte.models.periodic_task import PeriodicTask
from featurebyte.models.task import Task
from featurebyte.utils.messaging import REDIS_URI
from featurebyte.utils.persistent import DATABASE_NAME, MONGO_URI

CELERY_TASK_COLLECTION = Task.collection_name()
CELERY_SCHEDULE_COLLECTION = PeriodicTask.collection_name()

celery = Celery(
    __name__,
    broker=REDIS_URI,
    backend=MONGO_URI,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    enable_utc=True,
)

# Celery configuration options:
# https://docs.celeryq.dev/en/stable/userguide/configuration.html

# celery behavior
celery.conf.task_track_started = True

# task queues and routing
celery.conf.task_routes = {
    "featurebyte.worker.task_executor.execute_cpu_task": {"queue": "cpu_task"},
    "featurebyte.worker.task_executor.execute_io_task": {"queue": "io_task"},
}

celery.conf.broker_transport_options = {
    "priority_steps": list(range(3)),
    "sep": ":",
    "queue_order_strategy": "priority",
}

# task tombstones options
celery.conf.result_extended = True
celery.conf.result_expires = 15  # 15 days expiry
celery.conf.result_backend_always_retry = False
celery.conf.mongodb_backend_settings = {
    "database": DATABASE_NAME,
    "taskmeta_collection": CELERY_TASK_COLLECTION,
}

# beat scheduler
celery.conf.mongodb_scheduler_db = DATABASE_NAME
celery.conf.mongodb_scheduler_collection = CELERY_SCHEDULE_COLLECTION
celery.conf.mongodb_scheduler_url = MONGO_URI
