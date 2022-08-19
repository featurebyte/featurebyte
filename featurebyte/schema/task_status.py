"""
TaskStatus API payload schema
"""
from typing import Any, List, Literal, Union

from uuid import UUID

from beanie import PydanticObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.routes.common.schema import PaginationMixin


class TaskStatus(FeatureByteBaseModel):
    """
    TaskStatus retrieval schema
    """

    id: Union[PydanticObjectId, UUID] = Field(allow_mutation=False)
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
    result: Any


class TaskStatusList(PaginationMixin):
    """
    Paginated list of TaskStatus
    """

    data: List[TaskStatus]
