"""
This module contains TaskExecutor class
"""

from __future__ import annotations

from typing import Any, Coroutine, Optional, Set

import asyncio
import os
import time
from abc import abstractmethod
from concurrent.futures import TimeoutError as ConcurrentTimeoutError
from datetime import datetime
from threading import Thread
from unittest.mock import patch
from uuid import UUID

from bson import ObjectId
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded, WorkerTerminate
from celery.worker import state as worker_state

from featurebyte.config import Configurations, get_home_path
from featurebyte.enum import WorkerCommand
from featurebyte.exception import TaskCanceledError, TaskRevokeExceptions
from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.models.task import Task as TaskModel
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.utils.messaging import Progress
from featurebyte.utils.persistent import MongoDBImpl
from featurebyte.worker import get_celery
from featurebyte.worker.registry import TASK_REGISTRY_MAP
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


ASYNCIO_LOOP: asyncio.AbstractEventLoop | None = None
EVENT_LOOP_HEALTH_LAST_CHECK_TIME: float | None = None
EVENT_LOOP_HEALTH_CHECK_INTERVAL = 10  # how often to check the event loop health in seconds
EVENT_LOOP_HEALTH_CHECK_THRESHOLD = max(
    EVENT_LOOP_HEALTH_CHECK_INTERVAL + 5, 60
)  # loop unhealthy threshold (must be > EVENT_LOOP_HEALTH_CHECK_INTERVAL)


async def _loop_health_tracker() -> None:
    """
    Health tracker for the background loop
    """
    global EVENT_LOOP_HEALTH_LAST_CHECK_TIME  # pylint: disable=global-statement
    while True:
        EVENT_LOOP_HEALTH_LAST_CHECK_TIME = time.time()
        await asyncio.sleep(EVENT_LOOP_HEALTH_CHECK_INTERVAL)


def _check_asyncio_loop() -> asyncio.AbstractEventLoop:
    """
    Check asyncio loop and kill worker if not responsive

    Returns
    -------
    asyncio.AbstractEventLoop
        asyncio event loop

    Raises
    ------
    WorkerTerminate
        Worker is not responsive
    """
    global EVENT_LOOP_HEALTH_LAST_CHECK_TIME  # pylint: disable=global-statement
    assert ASYNCIO_LOOP is not None, "async loop is not initialized"

    if EVENT_LOOP_HEALTH_LAST_CHECK_TIME is None:
        # initialize health check
        EVENT_LOOP_HEALTH_LAST_CHECK_TIME = time.time()
        asyncio.run_coroutine_threadsafe(_loop_health_tracker(), ASYNCIO_LOOP)

    # kill worker if event loop is not responsive
    if time.time() - EVENT_LOOP_HEALTH_LAST_CHECK_TIME > EVENT_LOOP_HEALTH_CHECK_THRESHOLD:
        logger.error("Event loop is not responsive, shutting down")
        # attempt to cancel all tasks where possible
        task_ids = set()
        for task in asyncio.all_tasks(loop=ASYNCIO_LOOP):
            task.cancel()
            task_ids.add(task.get_name())
        # raise exception to terminate worker
        raise WorkerTerminate(True)

    return ASYNCIO_LOOP


def _revoke(
    state: Any, task_ids: Set[str], terminate: bool = False, signal: Any = None, **kwargs: Any
) -> Set[str]:
    """
    Cancel tasks running on the event loop

    Parameters
    ----------
    state: Any
        State of the worker
    task_ids: Set[str]
        Task IDs
    terminate: bool
        Terminate flag
    signal: Any
        Signal
    kwargs: Any
        Keyword arguments

    Returns
    -------
    Set[str]
        Task IDs
    """
    _ = state, signal, kwargs

    # mark task as revoked
    worker_state.revoked.update(task_ids)

    # check that asyncio loop is running and healthy
    _check_asyncio_loop()

    active_tasks = asyncio.all_tasks(loop=ASYNCIO_LOOP)
    if terminate:
        logger.debug("Revoking task", extra={"task_ids": task_ids})
        for task in active_tasks:
            if task.get_name() in task_ids:
                task.cancel()
    return task_ids


def run_async(
    coro: Coroutine[Any, Any, Any], request_id: UUID, timeout: Optional[int] = None
) -> Any:
    """
    Run async function in both async and non-async context
    Parameters
    ----------
    coro: Coroutine[Any, Any, Any]
        Coroutine to run
    request_id: UUID
        Request ID
    timeout: Optional[int]
        Timeout in seconds, default to None (no timeout)

    Returns
    -------
    Any
        result from function call

    Raises
    ------
    SoftTimeLimitExceeded
        timeout is exceeded
    """
    # check that asyncio loop is running and healthy
    loop = _check_asyncio_loop()

    with patch("celery.worker.control._revoke") as revoke:
        revoke.side_effect = _revoke
        logger.debug(
            "Running async function",
            extra={"timeout": timeout, "active_tasks": len(asyncio.all_tasks(loop=ASYNCIO_LOOP))},
        )

        task: Any = loop.create_task(coro, name=str(request_id))

        async def _run_task() -> Any:
            return await task

        future = asyncio.run_coroutine_threadsafe(_run_task(), loop)
        try:
            return future.result(timeout=timeout)
        except ConcurrentTimeoutError as exc:
            task.cancel()
            raise SoftTimeLimitExceeded(f"Task timed out after {timeout}s") from exc


class TaskExecutor:
    """
    TaskExecutor class
    """

    command_type = WorkerCommand

    def __init__(
        self,
        payload: dict[str, Any],
        task_id: UUID,
        app_container: LazyAppContainer,
    ) -> None:
        self.task_id = task_id
        command = self.command_type(payload["command"])
        self.persistent = app_container.get("persistent")
        self.user = User(id=payload.get("user_id"))
        self.task = app_container.get(TASK_REGISTRY_MAP[command])
        self.task_progress_updater = app_container.get(TaskProgressUpdater)
        self.task_manager = app_container.task_manager
        self.setup_worker_config()
        self.payload_dict = payload

    async def _update_task_start_time_and_description(self, payload: Any) -> None:
        """
        Update task start time and description

        Parameters
        ----------
        payload: Any
            Task payload
        """
        await self.persistent.update_one(
            collection_name=TaskModel.collection_name(),
            query_filter={"_id": str(self.task_id)},
            update={
                "$set": {
                    "start_time": datetime.utcnow(),
                    "description": await self.task.get_task_description(payload),
                }
            },
            disable_audit=True,
            user_id=self.user.id,
        )

    @staticmethod
    def setup_worker_config() -> None:
        """
        Setup featurebyte config file for the worker
        """
        home_path = get_home_path()
        if not home_path.exists():
            home_path.mkdir(parents=True)

        # override config file of the featurebyte-worker
        featurebyte_server = os.environ.get("FEATUREBYTE_SERVER", "http://featurebyte-server:8088")
        config_path = home_path.joinpath("config.yaml")
        config_path.write_text(
            "# featurebyte-worker config file\n"
            "profile:\n"
            "  - name: worker\n"
            f"    api_url: {featurebyte_server}\n\n"
            "default_profile: worker\n\n",
            encoding="utf-8",
        )
        # Reload newly written configuration
        Configurations(force=True)

    async def execute(self) -> Any:
        """
        Execute the task

        Raises
        ------
        TaskCanceledError
            Task revoked.
        """
        # Send initial progress to indicate task is started
        await self.task_progress_updater.update_progress(percent=0)
        payload_obj = self.task.get_payload_obj(self.payload_dict)

        try:
            # Execute the task
            await self._update_task_start_time_and_description(payload_obj)
            task_result = await self.task.execute(payload_obj)
            if task_result is not None:
                await self.task_manager.update_task_result(
                    task_id=str(self.task_id), result=task_result
                )

            # Send final progress to indicate task is completed
            await self.task_progress_updater.update_progress(percent=100)
        except TaskRevokeExceptions as exc:
            await self.task.handle_task_revoke(payload_obj)
            raise TaskCanceledError("Task canceled.") from exc


class BaseCeleryTask(Task):
    """
    Base Celery task
    """

    name = "base_task"
    progress_class = Progress
    executor_class = TaskExecutor

    @staticmethod
    async def get_app_container(
        task_id: UUID, payload: dict[str, Any], progress: Any
    ) -> LazyAppContainer:
        """
        Get app container

        Parameters
        ----------
        task_id: UUID
            Task ID
        payload: dict[str, Any]
            Task payload
        progress: Any
            Task progress

        Returns
        -------
        LazyAppContainer
        """
        instance_map_to_use = {
            # Default instances
            "celery": get_celery(),
            "persistent": MongoDBImpl(),
            # Task specific parameters
            "user": User(id=payload.get("user_id")),
            "catalog_id": ObjectId(payload.get("catalog_id")),
            "task_id": task_id,
            "payload": payload,
            "progress": progress,
        }
        return LazyAppContainer(
            app_container_config=app_container_config,
            instance_map=instance_map_to_use,
        )

    async def execute_task(self: Any, request_id: UUID, **payload: Any) -> Any:
        """
        Execute Celery task

        Parameters
        ----------
        request_id: UUID
            Request ID
        payload: Any
            Task payload

        Returns
        -------
        Any
        """
        command = str(payload.get("command"))
        logger.debug(f"Executing: {command}")
        progress = self.progress_class(user_id=payload.get("user_id"), task_id=request_id)
        app_container = await self.get_app_container(request_id, payload, progress)
        executor = self.executor_class(
            payload=payload, task_id=request_id, app_container=app_container
        )
        try:
            return_val = await executor.execute()
            return return_val
        finally:
            # indicate stream is closed
            progress.put({"percent": -1})

    @abstractmethod
    def run(self: Any, *args: Any, **payload: Any) -> Any:
        """
        Execute Celery task

        Parameters
        ----------
        args: Any
            Task arguments
        payload: Any
            Task payload

        Returns
        -------
        Any
        """
        raise NotImplementedError


class IOBoundTask(BaseCeleryTask):
    """
    Celery task for IO bound task
    """

    name = "featurebyte.worker.task_executor.execute_io_task"

    def run(self: Any, *args: Any, **payload: Any) -> Any:
        return run_async(
            self.execute_task(self.request.id, **payload),
            request_id=self.request.id,
            timeout=self.request.timelimit[1],
        )


class CPUBoundTask(BaseCeleryTask):
    """
    Celery task for CPU bound task
    """

    name = "featurebyte.worker.task_executor.execute_cpu_task"

    def run(self: Any, *args: Any, **payload: Any) -> Any:
        return asyncio.run(self.execute_task(self.request.id, **payload))


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
    global ASYNCIO_LOOP  # pylint: disable=global-statement
    ASYNCIO_LOOP = asyncio.new_event_loop()
    thread = Thread(target=start_background_loop, args=(ASYNCIO_LOOP,), daemon=True)
    thread.start()
