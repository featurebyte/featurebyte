"""
This module contains TaskExecutor class
"""
from __future__ import annotations

from typing import Any

import asyncio

from featurebyte.enum import WorkerCommand
from featurebyte.utils.credential import get_credential
from featurebyte.utils.persistent import get_persistent
from featurebyte.worker.task.base import TASK_MAP


class TaskExecutor:
    """
    TaskExecutor class
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, payload: dict[str, Any], progress: Any = None) -> None:
        command = WorkerCommand(payload["command"])
        task = TASK_MAP[command](
            payload=payload,
            progress=progress,
            get_persistent=get_persistent,
            get_credential=get_credential,
        )
        asyncio.run(task.execute())
