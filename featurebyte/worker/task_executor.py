"""
This module contains TaskExecutor class
"""
from __future__ import annotations

from typing import Any

import asyncio
import json
import traceback

from featurebyte.enum import WorkerCommand
from featurebyte.utils.credential import get_credential
from featurebyte.utils.persistent import get_persistent
from featurebyte.worker.task.base import TASK_MAP, BaseTask


class TaskExecutor:
    """
    TaskExecutor class
    """

    # pylint: disable=too-few-public-methods

    command_type = WorkerCommand

    def __init__(self, payload: dict[str, Any], queue: Any, progress: Any = None) -> None:
        command = self.command_type(payload["command"])
        task = TASK_MAP[command](
            payload=payload,
            progress=progress,
            get_persistent=get_persistent,
            get_credential=get_credential,
        )
        asyncio.run(self.execute(task, queue))

    @staticmethod
    async def execute(task: BaseTask, queue: Any) -> None:
        """
        Execute the task

        Parameters
        ----------
        task: BaseTask
            Task object
        queue: Any
            Queue used to passed process output
        """
        try:
            output = await task.execute()
            queue.put(json.dumps({"output": output}))
        except Exception:  # pylint: disable=broad-except
            traceback_info = traceback.format_exc()
            queue.put(json.dumps({"traceback": traceback_info}))
