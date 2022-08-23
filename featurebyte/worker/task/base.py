"""
Base models for task and task payload
"""
from __future__ import annotations

from typing import Any

from abc import abstractmethod
from enum import Enum

from featurebyte.schema.worker.progress import ProgressModel
from featurebyte.schema.worker.task.base import BaseTaskPayload

TASK_MAP = {}


class BaseTask:
    """
    Base class for Task
    """

    # pylint: disable=too-few-public-methods

    payload_class: type[BaseTaskPayload] = BaseTaskPayload

    def __init__(
        self,
        payload: dict[str, Any],
        progress: Any = None,
        get_persistent: Any = None,
        get_credential: Any = None,
    ):
        if self.payload_class == BaseTaskPayload:
            raise NotImplementedError
        self.payload = self.payload_class(**payload)
        self.get_persistent = get_persistent
        self.get_credential = get_credential
        self.progress = progress

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        assert isinstance(cls.payload_class.command, Enum)
        command = cls.payload_class.command
        if command in TASK_MAP:
            raise ValueError(f'Command "{command}" has been implemented.')
        TASK_MAP[command] = cls

    def update_progress(self, percent: int, message: str | None = None) -> None:
        """
        Update progress

        Parameters
        ----------
        percent: int
            Completed progress percentage
        message: str | None
            Optional message
        """
        if self.progress:
            progress = ProgressModel(percent=percent, message=message)
            self.progress.put(progress.dict(exclude_none=True))

    @abstractmethod
    async def execute(self) -> Any:
        """
        Execute the task
        """
