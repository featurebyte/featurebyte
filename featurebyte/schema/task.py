"""
TaskStatus API payload schema
"""

import datetime
import uuid
from typing import Any, Dict, List, Optional, Set, Union

from pydantic import AfterValidator, Field
from typing_extensions import Annotated

from featurebyte.enum import StrEnum
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.task import ProgressHistory
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
    def non_terminal(cls) -> Set[StrEnum]:
        """
        Non terminal status values

        Returns
        -------
        Set[StrEnum]
        """
        return {cls.PENDING, cls.RECEIVED, cls.STARTED, cls.RETRY}

    @classmethod
    def terminal(cls) -> Set[StrEnum]:
        """
        Terminal status values

        Returns
        -------
        Set[StrEnum]
        """
        return {cls.SUCCESS, cls.FAILURE, cls.REVOKED, cls.REJECTED, cls.IGNORED}

    @classmethod
    def unsuccessful(cls) -> Set[StrEnum]:
        """
        Unsuccessful status values

        Returns
        -------
        Set[StrEnum]
        """
        return {cls.FAILURE, cls.REVOKED, cls.REJECTED, cls.IGNORED}


class Task(FeatureByteBaseModel):
    """
    TaskStatus retrieval schema
    """

    id: TaskId = Field(frozen=True)
    status: TaskStatus = Field(frozen=True)
    output_path: Optional[str] = Field(default=None)
    payload: Dict[str, Any]
    traceback: Optional[str] = Field(default=None)
    start_time: Optional[datetime.datetime] = Field(frozen=True, default=None)
    date_done: Optional[datetime.datetime] = Field(frozen=True, default=None)
    progress: Optional[Dict[str, Any]] = Field(default=None)
    progress_history: Optional[ProgressHistory] = Field(default=None)
    child_task_ids: Optional[List[TaskId]] = Field(default=None)
    queue: Optional[str] = Field(default=None)


class TaskList(PaginationMixin):
    """
    Paginated list of TaskStatus
    """

    data: List[Task]


class TaskUpdate(FeatureByteBaseModel):
    """
    Update Task
    """

    revoke: Optional[bool] = Field(default=None)
