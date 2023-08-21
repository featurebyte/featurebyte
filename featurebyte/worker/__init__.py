"""
Celery worker
"""
from __future__ import annotations

from typing import Any

from bson.errors import InvalidDocument
from celery import Celery
from celery.backends.mongodb import MongoBackend
from kombu.exceptions import EncodeError
from redis.client import Redis

from featurebyte.models.periodic_task import PeriodicTask
from featurebyte.models.task import Task
from featurebyte.utils.messaging import REDIS_URI
from featurebyte.utils.persistent import DATABASE_NAME, MONGO_URI


def get_redis(redis_uri: str = REDIS_URI) -> Redis[Any]:
    """
    Get Redis instance

    Parameters
    ----------
    redis_uri: str
        Redis URI

    Returns
    -------
    Redis[Any]
    """
    return Redis.from_url(redis_uri)


def format_mongo_uri_for_scheduler(mongo_uri: str) -> str:
    """
    Format mongo uri to be compatible with celery beat scheduler

    Parameters
    ----------
    mongo_uri: str
        Mongo URI

    Returns
    -------
    str
    """
    # mongodb://.../admin ->  mongodb://.../?authSource=admin
    if "/admin" in mongo_uri:
        mongo_uri = mongo_uri.replace("/admin", "/")
        if "?" in mongo_uri:
            mongo_uri = mongo_uri.replace("?", "?authSource=admin&")
            if mongo_uri.endswith("&"):
                mongo_uri = mongo_uri[:-1]
        else:
            mongo_uri += "?authSource=admin"
    return mongo_uri


class ExtendedMongoBackend(MongoBackend):
    """
    Extend MongoBackend class to update mongodb document instead of replacing it
    """

    def add_to_chord(self, chord_id: Any, result: Any) -> Any:
        raise NotImplementedError("Backend does not support add_to_chord")

    def _store_result(
        self,
        task_id: str,
        result: Any,
        state: str,
        traceback: Any = None,
        request: Any = None,
        **kwargs: Any,
    ) -> Any:
        meta = self._get_result_meta(
            result=self.encode(result), state=state, traceback=traceback, request=request
        )
        # Add the _id for mongodb
        meta["_id"] = task_id

        try:
            self.collection.update_one({"_id": task_id}, {"$set": meta}, upsert=True)
        except InvalidDocument as exc:
            raise EncodeError(exc) from exc

        return result


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
    celery_app.conf.result_backend = f"featurebyte.worker:ExtendedMongoBackend+{mongo_uri}"
    celery_app.conf.broker_connection_retry_on_startup = True

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
    celery_app.conf.mongodb_scheduler_url = format_mongo_uri_for_scheduler(mongo_uri)

    return celery_app
