"""
Celery worker
"""

from __future__ import annotations

import asyncio
from threading import Thread
from typing import Any

from bson.errors import InvalidDocument
from celery import Celery
from celery.backends.mongodb import MongoBackend
from celery.worker.control import _revoke, control_command, ok
from kombu.exceptions import EncodeError
from kombu.utils import maybe_list
from redis.client import Redis

from featurebyte import logging
from featurebyte.models.periodic_task import PeriodicTask
from featurebyte.models.task import Task
from featurebyte.utils.messaging import REDIS_URI
from featurebyte.utils.persistent import DATABASE_NAME, MONGO_URI
from featurebyte.worker.loader import NoPrefetchTaskLoader

ASYNCIO_LOOP: asyncio.AbstractEventLoop | None = None

logger = logging.get_logger(__name__)


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


def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    """
    Start background event loop

    Parameters
    ----------
    loop: asyncio.AbstractEventLoop
        asyncio event loop
    """

    asyncio.set_event_loop(loop)
    try:
        loop.run_forever()
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()


def initialize_asyncio_event_loop() -> None:
    """
    Initialize asyncio event loop
    """
    logger.debug("Initializing asyncio event loop")
    global ASYNCIO_LOOP
    ASYNCIO_LOOP = asyncio.new_event_loop()
    thread = Thread(target=start_background_loop, args=(ASYNCIO_LOOP,), daemon=True)
    thread.start()


def get_async_loop() -> asyncio.AbstractEventLoop:
    """
    Get the asyncio event loop

    Returns
    -------
    asyncio.AbstractEventLoop
    """
    assert ASYNCIO_LOOP is not None, "async loop is not initialized"
    return ASYNCIO_LOOP


@control_command(
    variadic="task_id",
    signature="[id1 [id2 [... [idN]]]]",
)
def revoke(
    state: Any, task_id: Any, terminate: bool = False, signal: Any = None, **kwargs: Any
) -> Any:
    """
    Override revoke to cancel asyncio tasks if terminate is True.

    Parameters
    ----------
    state: Any
        State
    task_id: Any
        Task ID
    terminate: bool
        Terminate flag
    signal: Any
        Signal
    kwargs: Any
        Keyword arguments

    Returns
    -------
    Any
    """
    task_ids: Any = set(maybe_list(task_id) or [])
    try:
        task_ids = _revoke(state, task_ids, terminate, signal, **kwargs)
    except NotImplementedError:
        if terminate:
            logger.debug("Revoking task", extra={"task_ids": task_ids})
            active_tasks = asyncio.all_tasks(loop=ASYNCIO_LOOP)
            for task in active_tasks:
                if task.get_name() in task_ids:
                    task.cancel()
    if isinstance(task_ids, dict) and "ok" in task_ids:
        return task_ids
    return ok(f"tasks {task_ids} flagged as revoked")


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
        loader=NoPrefetchTaskLoader,
    )

    # Celery configuration options:
    # https://docs.celeryq.dev/en/stable/userguide/configuration.html

    # celery behavior
    celery_app.conf.task_track_started = True
    celery_app.conf.result_backend = f"featurebyte.worker:ExtendedMongoBackend+{mongo_uri}"
    celery_app.conf.broker_connection_retry_on_startup = True
    celery_app.conf.worker_prefetch_multiplier = 1

    # task queues and routing
    celery_app.conf.task_routes = {
        "featurebyte.worker.task_executor.execute_cpu_task": {"queue": "cpu_task"},
        "featurebyte.worker.task_executor.execute_io_task": {"queue": "io_task"},
    }

    celery_app.conf.broker_transport_options = {
        "priority_steps": list(range(3)),
        "sep": ":",
        "queue_order_strategy": "priority",
        "visibility_timeout": 3600 * 24,
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
