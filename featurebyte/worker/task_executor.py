"""
This module contains TaskExecutor class
"""
from __future__ import annotations

from typing import Any

import asyncio

from featurebyte.worker.task.base import TASK_MAP


class TaskExecutor:
    """
    TaskExecutor class
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, payload: dict[str, Any], progress: Any = None) -> None:
        command = payload["command"]
        task = TASK_MAP[command](payload=payload, progress=progress)
        asyncio.run(task.execute())
