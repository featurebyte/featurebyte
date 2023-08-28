"""
This module contains TaskExecutor class
"""
from __future__ import annotations

from typing import Any, Awaitable, Optional

import asyncio
import os
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Thread
from uuid import UUID

import gevent
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded

from featurebyte.config import Configurations, get_home_path
from featurebyte.enum import WorkerCommand
from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.models.task import Task as TaskModel
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.utils.credential import MongoBackedCredentialProvider
from featurebyte.utils.messaging import Progress
from featurebyte.utils.persistent import get_persistent
from featurebyte.utils.storage import get_storage, get_temp_storage
from featurebyte.worker import get_celery, get_redis
from featurebyte.worker.task.base import TASK_MAP

logger = get_logger(__name__)


def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    """
    Start background event loop

    Parameters
    ----------
    loop: AbstractEventLoop
        Event loop to run
    """
    asyncio.set_event_loop(loop)
    loop.run_forever()


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
    try:
        loop = asyncio.get_running_loop()
        logger.debug("Use existing async loop", extra={"loop": loop})
    except RuntimeError:
        loop = asyncio.new_event_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1000))
        logger.debug("Create new async loop", extra={"loop": loop})
        thread = Thread(target=start_background_loop, args=(loop,), daemon=True)
        thread.start()

    logger.debug("Asyncio tasks", extra={"num_tasks": len(asyncio.all_tasks(loop=loop))})

    logger.info("Start task", extra={"timeout": timeout})
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        with gevent.Timeout(seconds=timeout, exception=TimeoutError):
            event = gevent.event.Event()
            future.add_done_callback(lambda _: event.set())
            event.wait()
            return future.result()
    except TimeoutError as exc:
        # try to cancel the job if it has not started
        future.cancel()
        raise SoftTimeLimitExceeded(f"Task timed out after {timeout}s") from exc


class TaskExecutor:
    """
    TaskExecutor class
    """

    command_type = WorkerCommand

    def __init__(self, payload: dict[str, Any], task_id: UUID, progress: Any = None) -> None:
        self.task_id = task_id
        command = self.command_type(payload["command"])
        credential_provider = MongoBackedCredentialProvider(persistent=get_persistent())
        user = User(id=payload.get("user_id"))
        task_class = TASK_MAP[command]
        payload_object = task_class.payload_class(**payload)
        app_container = LazyAppContainer(
            user=user,
            persistent=get_persistent(),
            temp_storage=get_temp_storage(),
            celery=get_celery(),
            redis=get_redis(),
            storage=get_storage(),
            catalog_id=payload_object.catalog_id,
            app_container_config=app_container_config,
        )
        self.task = task_class(
            task_id=task_id,
            payload=payload,
            progress=progress,
            get_credential=credential_provider.get_credential,
            app_container=app_container,
        )
        self._setup_worker_config()

    async def _update_task_start_time_and_description(self, persistent: Any) -> None:
        """
        Update task start time and description

        Parameters
        ----------
        persistent: Any
            Persistent object
        """
        await persistent.update_one(
            collection_name=TaskModel.collection_name(),
            query_filter={"_id": str(self.task_id)},
            update={
                "$set": {
                    "start_time": datetime.utcnow(),
                    "description": await self.task.get_task_description(),
                }
            },
            disable_audit=True,
            user_id=self.task.user.id,
        )

    def _setup_worker_config(self) -> None:
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
        """
        await self._update_task_start_time_and_description(self.task.persistent)
        await self.task.execute()


class BaseCeleryTask(Task):
    """
    Base Celery task
    """

    name = "base_task"
    progress_class = Progress
    executor_class = TaskExecutor

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
        executor = self.executor_class(payload=payload, task_id=request_id, progress=progress)
        # send initial progress to indicate task is started
        await executor.task.update_progress(percent=0)
        try:
            return_val = await executor.execute()
            # send final progress to indicate task is completed
            await executor.task.update_progress(percent=100)
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
