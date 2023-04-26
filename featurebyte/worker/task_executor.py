"""
This module contains TaskExecutor class
"""
from __future__ import annotations

from typing import Any

import asyncio

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import User
from featurebyte.utils.credential import MongoBackedCredentialProvider
from featurebyte.utils.messaging import Progress
from featurebyte.utils.persistent import get_persistent
from featurebyte.utils.storage import get_storage, get_temp_storage
from featurebyte.worker import celery
from featurebyte.worker.task.base import TASK_MAP


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


@celery.task(bind=True)
def execute_task(self: Any, **payload: Any) -> None:
    """
    Execute Celery task

    Parameters
    ----------
    self: Any
        Celery Task
    payload: Any
        Task payload
    """
    progress = Progress(user_id=payload.get("user_id"), task_id=self.request.id)
    executor = TaskExecutor(payload=payload, progress=progress)
    # send initial progress to indicate task is started
    progress.put({"percent": 0})
    try:
        asyncio.run(executor.execute())
        # send final progress to indicate task is completed
        progress.put({"percent": 100})
    finally:
        # indicate stream is closed
        progress.put({"percent": -1})
