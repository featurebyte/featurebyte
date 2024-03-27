"""
TaskStatus API payload schema
"""

from typing import Any, Dict, List, Optional, Set, Union

import datetime
from uuid import UUID

from pydantic import Field

from featurebyte.enum import StrEnum
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.schema.common.base import PaginationMixin

TaskId = Union[PydanticObjectId, UUID]


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


class Task(FeatureByteBaseModel):
    """
    TaskStatus retrieval schema
    """

    id: TaskId = Field(allow_mutation=False)
    status: TaskStatus = Field(allow_mutation=False)
    output_path: Optional[str]
    payload: Dict[str, Any]
    traceback: Optional[str]
    start_time: Optional[datetime.datetime]
    date_done: Optional[datetime.datetime]
    progress: Optional[Dict[str, Any]] = Field(default=None)


class TaskList(PaginationMixin):
    """
    Paginated list of TaskStatus
    """

    data: List[Task]


class TaskUpdate(FeatureByteBaseModel):
    """
    Update Task
    """

    revoke: Optional[bool]
