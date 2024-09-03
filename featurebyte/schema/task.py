"""
TaskStatus API payload schema
"""

import datetime
import uuid
from typing import Annotated, Any, Optional, Union

from pydantic import AfterValidator, Field

from featurebyte.enum import StrEnum
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.schema.common.base import PaginationMixin

UUID4 = Union[uuid.UUID, Annotated[str, AfterValidator(lambda x: uuid.UUID(x, version=4))]]
TaskId = Union[UUID4, PydanticObjectId]


class TaskStatus(StrEnum):
    """
    TaskStatus enum
    """

    PENDING = "PENDING"
    RECEIVED = "RECEIVED"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    REVOKED = "REVOKED"
    REJECTED = "REJECTED"
    RETRY = "RETRY"
    IGNORED = "IGNORED"

    @classmethod
    def non_terminal(cls) -> set[StrEnum]:
        """
        Non terminal status values

        Returns
        -------
        Set[StrEnum]
        """
        return {cls.PENDING, cls.RECEIVED, cls.STARTED, cls.RETRY}

    @classmethod
    def terminal(cls) -> set[StrEnum]:
        """
        Terminal status values

        Returns
        -------
        Set[StrEnum]
        """
        return {cls.SUCCESS, cls.FAILURE, cls.REVOKED, cls.REJECTED, cls.IGNORED}


class Task(FeatureByteBaseModel):
    """
    TaskStatus retrieval schema
    """

    id: TaskId = Field(frozen=True)
    status: TaskStatus = Field(frozen=True)
    output_path: Optional[str] = Field(default=None)
    payload: dict[str, Any]
    traceback: Optional[str] = Field(default=None)
    start_time: Optional[datetime.datetime] = Field(frozen=True, default=None)
    date_done: Optional[datetime.datetime] = Field(frozen=True, default=None)
    progress: Optional[dict[str, Any]] = Field(default=None)
    child_task_ids: Optional[list[TaskId]] = Field(default=None)


class TaskList(PaginationMixin):
    """
    Paginated list of TaskStatus
    """

    data: list[Task]


class TaskUpdate(FeatureByteBaseModel):
    """
    Update Task
    """

    revoke: Optional[bool] = Field(default=None)
