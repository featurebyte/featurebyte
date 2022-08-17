"""
Base models for task and task payload
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional

from abc import abstractmethod

from beanie import PydanticObjectId
from pydantic import BaseModel

from featurebyte.worker.enum import Command

# contains command to task class mapping
TASK_MAP = {}
TASK_PAYLOAD_MAP = {}


class BaseTaskPayload(BaseModel):
    """
    Base class for Task payload
    """

    user_id: PydanticObjectId
    document_id: PydanticObjectId
    collection_name: ClassVar[Optional[str]] = None
    command: ClassVar[Optional[Command]] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        TASK_PAYLOAD_MAP[cls.command] = cls

    @property
    def redirect_route(self) -> Optional[str]:
        """
        Redirect route used to retrieve the task result

        Returns
        -------
        Optional[str]
        """
        return f"{self.collection_name}/{self.document_id}" if self.collection_name else None

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        output: dict[str, Any] = super().dict(*args, **kwargs)
        if self.command:
            output["command"] = self.command.value
        return output


class BaseTask:
    """
    Base class for Task
    """

    # pylint: disable=too-few-public-methods

    payload_class: type[BaseTaskPayload] = BaseTaskPayload
    command: Command

    def __init__(self, payload: dict[str, Any]):
        if self.command is None or self.payload_class is None:
            raise NotImplementedError
        self.payload = self.payload_class(**payload)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        TASK_MAP[cls.command] = cls

    @abstractmethod
    def execute(self) -> None:
        """
        Execute the task
        """
