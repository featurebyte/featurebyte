"""
TaskStatus API payload schema
"""
from typing import Any, Dict, List, Optional, Union

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


class Task(FeatureByteBaseModel):
    """
    TaskStatus retrieval schema
    """

    id: TaskId = Field(allow_mutation=False)
    status: TaskStatus = Field(allow_mutation=False)
    output_path: Optional[str]
    payload: Dict[str, Any]
    traceback: Optional[str]


class TaskList(PaginationMixin):
    """
    Paginated list of TaskStatus
    """

    data: List[Task]
