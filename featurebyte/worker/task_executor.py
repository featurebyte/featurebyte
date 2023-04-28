"""
This module contains TaskExecutor class
"""
from __future__ import annotations

from typing import Any

import asyncio

import gevent
import gevent.event

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import User
from featurebyte.utils.credential import MongoBackedCredentialProvider
from featurebyte.utils.messaging import Progress
from featurebyte.utils.persistent import get_persistent
from featurebyte.utils.storage import get_storage, get_temp_storage
from featurebyte.worker import celery
from featurebyte.worker.task.base import TASK_MAP


def run_async(func: Any, *args: Any, **kwargs: Any) -> Any:
    """
    Run async function in both async and non-async context
    Parameters
    ----------
    func: Any
        Function to run
    args: Any
        Positional arguments
    kwargs: Any
        Keyword arguments
    Returns
    -------
    Any
        result from function call
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    future = loop.create_task(func(*args, **kwargs))
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


async def execute_task(self: Any, **payload: Any) -> Any:
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
    progress = Progress(user_id=payload.get("user_id"), task_id=self.request.id)
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
    return asyncio.run(execute_task(self, **payload))


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
    return asyncio.run(execute_task(self, **payload))
