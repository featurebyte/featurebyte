"""
Celery worker
"""
from celery import Celery

from featurebyte.models.periodic_task import PeriodicTask
from featurebyte.models.task import Task
from featurebyte.utils.messaging import REDIS_URI
from featurebyte.utils.persistent import DATABASE_NAME, MONGO_URI


def get_celery(
    redis_uri: str = REDIS_URI, mongo_uri: str = MONGO_URI, database_name: str = DATABASE_NAME
) -> Celery:
    """
    Get Celery instance

    Parameters
    ----------
    redis_uri: str
        Redis URI
    mongo_uri: str
        Mongo URI
    database_name: str
        Database name

    Returns
    -------
    Celery
    """
    celery_app = Celery(
        __name__,
        broker=redis_uri,
        backend=mongo_uri,
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        enable_utc=True,
    )

    # Celery configuration options:
    # https://docs.celeryq.dev/en/stable/userguide/configuration.html

    # celery behavior
    celery_app.conf.task_track_started = True

    # task queues and routing
    celery_app.conf.task_routes = {
        "featurebyte.worker.task_executor.execute_cpu_task": {"queue": "cpu_task"},
        "featurebyte.worker.task_executor.execute_io_task": {"queue": "io_task"},
    }

    celery_app.conf.broker_transport_options = {
        "priority_steps": list(range(3)),
        "sep": ":",
        "queue_order_strategy": "priority",
    }

    # task tombstones options
    celery_app.conf.result_extended = True
    celery_app.conf.result_expires = 15  # 15 days expiry
    celery_app.conf.result_backend_always_retry = False
    celery_app.conf.mongodb_backend_settings = {
        "database": database_name,
        "taskmeta_collection": Task.collection_name(),
    }

    # beat scheduler
    celery_app.conf.mongodb_scheduler_db = database_name
    celery_app.conf.mongodb_scheduler_collection = PeriodicTask.collection_name()
    celery_app.conf.mongodb_scheduler_url = mongo_uri

    return celery_app
