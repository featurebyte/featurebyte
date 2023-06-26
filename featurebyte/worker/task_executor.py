"""
This module contains TaskExecutor class
"""
from __future__ import annotations

from typing import Any, Awaitable, Optional

import asyncio
import os
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from uuid import UUID

import gevent
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded

from featurebyte.config import get_home_path
from featurebyte.enum import WorkerCommand
from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.utils.credential import MongoBackedCredentialProvider
from featurebyte.utils.messaging import Progress
from featurebyte.utils.persistent import get_persistent
from featurebyte.utils.storage import get_storage, get_temp_storage
from featurebyte.worker import get_celery
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

    def __init__(self, payload: dict[str, Any], progress: Any = None) -> None:
        command = self.command_type(payload["command"])
        credential_provider = MongoBackedCredentialProvider(persistent=get_persistent())
        self.task = TASK_MAP[command](
            user=User(id=payload.get("user_id")),
            payload=payload,
            progress=progress,
            get_persistent=get_persistent,
            get_credential=credential_provider.get_credential,
            get_storage=get_storage,
            get_temp_storage=get_temp_storage,
            get_celery=get_celery,
        )
        self._setup_worker_config()

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

    async def execute(self) -> Any:
        """
        Execute the task
        """
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
        executor = self.executor_class(payload=payload, progress=progress)
        # send initial progress to indicate task is started
        progress.put({"percent": 0})
        try:
            return_val = await executor.execute()
            # send final progress to indicate task is completed
            progress.put({"percent": 100})
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
