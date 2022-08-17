"""
This module contains TaskExecutor class
"""
from __future__ import annotations

from typing import Any

from featurebyte.worker.task.base import TASK_MAP


class TaskExecutor:
    """
    TaskExecutor class
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, payload: dict[str, Any]) -> None:
        command = payload["command"]
        task = TASK_MAP[command](payload=payload)
        task.execute()
