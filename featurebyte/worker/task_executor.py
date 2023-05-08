"""
This module contains TaskExecutor class
"""
from __future__ import annotations

from typing import Any, Awaitable

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from uuid import UUID

import gevent

from featurebyte.enum import WorkerCommand
from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.utils.credential import MongoBackedCredentialProvider
from featurebyte.utils.messaging import Progress
from featurebyte.utils.persistent import get_persistent
from featurebyte.utils.storage import get_storage, get_temp_storage
from featurebyte.worker import celery
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


def run_async(coro: Awaitable[Any]) -> Any:
    """
    Run async function in both async and non-async context
    Parameters
    ----------
    coro: Coroutine
        Coroutine to run

    Returns
    -------
    Any
        result from function call
    """
    try:
        loop = asyncio.get_event_loop()
        logger.debug("Use existing async loop", extra={"loop": loop})
    except RuntimeError:
        loop = asyncio.new_event_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1000))
        logger.debug("Create new async loop", extra={"loop": loop})
        thread = Thread(target=start_background_loop, args=(loop,), daemon=True)
        thread.start()

    tasks = asyncio.all_tasks()
    logger.debug("Asyncio tasks", extra={"num_tasks": len(tasks)})
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    event = gevent.event.Event()
    future.add_done_callback(lambda _: event.set())
    event.wait()
    return future.result()


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
        )

    async def execute(self) -> Any:
        """
        Execute the task
        """
        await self.task.execute()


async def execute_task(request_id: UUID, **payload: Any) -> Any:
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
    progress = Progress(user_id=payload.get("user_id"), task_id=request_id)
    executor = TaskExecutor(payload=payload, progress=progress)
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


@celery.task(bind=True)
def execute_io_task(self: Any, **payload: Any) -> Any:
    """
    Execute Celery task

    Parameters
    ----------
    self: Any
        Celery Task
    payload: Any
        Task payload

    Returns
    -------
    Any
    """
    return run_async(execute_task(self.request.id, **payload))


@celery.task(bind=True)
def execute_cpu_task(self: Any, **payload: Any) -> Any:
    """
    Execute Celery task

    Parameters
    ----------
    self: Any
        Celery Task
    payload: Any
        Task payload

    Returns
    -------
    Any
    """
    return asyncio.run(execute_task(self.request.id, **payload))
