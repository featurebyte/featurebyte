"""
This module contains TaskExecutor class
"""
from __future__ import annotations

from typing import Any

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import User
from featurebyte.utils.credential import get_credential
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
        self.task = TASK_MAP[command](
            user=User(id=payload.get("user_id")),
            payload=payload,
            progress=progress,
            get_persistent=get_persistent,
            get_credential=get_credential,
            get_storage=get_storage,
            get_temp_storage=get_temp_storage,
        )

    async def execute(self) -> Any:
        """
        Execute the task
        """
        await self.task.execute()


@celery.task(bind=True)
async def execute_task(self: Any, **payload: Any) -> None:
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
    await executor.execute()
