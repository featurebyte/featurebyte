"""
This module contains TaskExecutor class
"""

from __future__ import annotations

import asyncio
import os
import time
from abc import abstractmethod
from concurrent.futures import TimeoutError as ConcurrentTimeoutError
from datetime import datetime
from typing import Any, Coroutine, Optional, Set
from uuid import UUID

from bson import ObjectId
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded, WorkerTerminate

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
from featurebyte.worker import get_async_loop, get_celery
from featurebyte.worker.registry import TASK_REGISTRY_MAP
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


PENDING_TASKS: Set[UUID] = set()
EXECUTION_LATENCY_THRESHOLD = 10  # max delay allowed for task execution in seconds
WORKER_TERMINATED = False


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
    WorkerTerminate
        Worker is terminated
    """
    loop = get_async_loop()
    task_received_time = time.time()

    logger.debug(
        "Running async function",
        extra={"timeout": timeout, "active_tasks": len(asyncio.all_tasks(loop=loop))},
    )

    async def _run_task() -> Any:
        return await loop.create_task(coro, name=str(request_id))

    PENDING_TASKS.add(request_id)
    future = asyncio.run_coroutine_threadsafe(_run_task(), loop)

    if timeout is None or timeout > EXECUTION_LATENCY_THRESHOLD:
        try:
            return future.result(timeout=EXECUTION_LATENCY_THRESHOLD)
        except ConcurrentTimeoutError as exc:
            # check if task execution is delayed beyond threshold
            if request_id in PENDING_TASKS:
                logger.error("Worker is not responsive, shutting down")
                raise WorkerTerminate(True) from exc
        # continue waiting for the task to complete
        if timeout is not None:
            timeout = max(timeout - int(time.time() - task_received_time), 0)

    try:
        return future.result(timeout=timeout)
    except ConcurrentTimeoutError as exc:
        active_tasks = asyncio.all_tasks(loop=loop)
        for task in active_tasks:
            if task.get_name() == str(request_id):
                task.cancel()
                break
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

        # store task_id in task object
        self.task.set_task_id(task_id)

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
            task = await self.task_manager.get_task(task_id=str(self.task_id))
            skip_last_progress_update = False
            if (
                task is not None
                and task.progress is not None
                and task.progress.get("percent") == 100
            ):
                # skip the last progress update if the progress is already 100%
                skip_last_progress_update = True

            if not skip_last_progress_update:
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
        print(
            f"Running: {command}"
        )  # Add temporary print statement to confirm logging not causing freezing issue
        logger.debug(f"Executing: {command}")
        if request_id in PENDING_TASKS:
            PENDING_TASKS.remove(request_id)

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
            await progress.put({"percent": -1})
            await progress.close()

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
        global WORKER_TERMINATED
        if WORKER_TERMINATED:
            raise WorkerTerminate(True)
        try:
            return run_async(
                self.execute_task(self.request.id, **payload),
                request_id=self.request.id,
                timeout=self.request.timelimit[1],
            )
        except WorkerTerminate as exc:
            WORKER_TERMINATED = True
            raise self.retry(countdown=0) from exc


class CPUBoundTask(BaseCeleryTask):
    """
    Celery task for CPU bound task
    """

    name = "featurebyte.worker.task_executor.execute_cpu_task"

    def run(self: Any, *args: Any, **payload: Any) -> Any:
        return asyncio.run(self.execute_task(self.request.id, **payload))
