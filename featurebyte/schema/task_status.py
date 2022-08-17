"""
TaskStatus API payload schema
"""
from typing import List, Literal

from beanie import PydanticObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.routes.common.schema import PaginationMixin


class TaskStatus(FeatureByteBaseModel):
    """
    TaskStatus retrieval schema
    """

    id: PydanticObjectId = Field(allow_mutation=False)
    status: Literal["running", "complete", "error", "not_found"] = Field(allow_mutation=False)


class TaskStatusList(PaginationMixin):
    """
    Paginated list of TaskStatus
    """

    data: List[TaskStatus]
