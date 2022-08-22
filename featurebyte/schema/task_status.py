"""
TaskStatus API payload schema
"""
from typing import Any, Dict, List, Literal, Union

from uuid import UUID

from beanie import PydanticObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.routes.common.schema import PaginationMixin

TaskId = Union[PydanticObjectId, UUID]


class TaskSubmission(FeatureByteBaseModel):
    """
    Task Submission schema
    """

    task_id: TaskId = Field(allow_mutation=False)
    output_document_id: PydanticObjectId = Field(allow_mutation=False)


class TaskStatus(FeatureByteBaseModel):
    """
    TaskStatus retrieval schema
    """

    id: TaskId = Field(allow_mutation=False)
    status: Literal[
        "PENDING",
        "RECEIVED",
        "STARTED",
        "SUCCESS",
        "FAILURE",
        "REVOKED",
        "REJECTED",
        "RETRY",
        "IGNORED",
    ] = Field(allow_mutation=False)
    output_path: str
    payload: Dict[str, Any]


class TaskStatusList(PaginationMixin):
    """
    Paginated list of TaskStatus
    """

    data: List[TaskStatus]
