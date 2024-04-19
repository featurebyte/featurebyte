"""
This module contains TaskExecutor class
"""

from __future__ import annotations

from typing import Any, Awaitable, Optional

import asyncio
import os
from abc import abstractmethod
from concurrent.futures import TimeoutError as ConcurrentTimeoutError
from datetime import datetime
from threading import Thread
from uuid import UUID

from bson import ObjectId
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded

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


def run_async(coro: Awaitable[Any], timeout: Optional[int] = None) -> Any:
    """
    Run async function in both async and non-async context
    Parameters
    ----------
    coro: Coroutine
        Coroutine to run
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
    logger.debug(
        "Running async function",
        extra={"timeout": timeout, "active_tasks": len(asyncio.all_tasks())},
    )
    assert ASYNCIO_LOOP is not None, "async loop is not initialized"
    future = asyncio.run_coroutine_threadsafe(coro, ASYNCIO_LOOP)
    try:
        return future.result(timeout=timeout)
    except ConcurrentTimeoutError as exc:
        # try to cancel the job if it has not started
        future.cancel()
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
            self.execute_task(self.request.id, **payload), timeout=self.request.timelimit[1]
        )


class CPUBoundTask(BaseCeleryTask):
    """
    Celery task for CPU bound task
    """

    name = "featurebyte.worker.task_executor.execute_cpu_task"

    def run(self: Any, *args: Any, **payload: Any) -> Any:
        return asyncio.run(self.execute_task(self.request.id, **payload))


def start_background_loop() -> None:
    """
    Start background event loop
    """
    global ASYNCIO_LOOP  # pylint: disable=global-statement
    ASYNCIO_LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(ASYNCIO_LOOP)
    try:
        ASYNCIO_LOOP.run_forever()
    finally:
        try:
            ASYNCIO_LOOP.run_until_complete(ASYNCIO_LOOP.shutdown_asyncgens())
        finally:
            ASYNCIO_LOOP.close()


def initialize_asyncio_event_loop() -> None:
    """
    Initialize asyncio event loop
    """
    logger.debug("Initializing asyncio event loop")
    thread = Thread(target=start_background_loop, daemon=True)
    thread.start()
