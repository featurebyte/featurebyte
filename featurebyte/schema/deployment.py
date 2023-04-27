"""
Pydantic schemas for handling API payloads for deployment routes
"""
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.deployment import DeploymentModel
from featurebyte.schema.common.base import (
    BaseDocumentServiceUpdateSchema,
    BaseInfo,
    PaginationMixin,
)


class DeploymentCreate(FeatureByteBaseModel):
    """
    Schema for deployment creation
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: Optional[StrictStr]
    feature_list_id: PydanticObjectId


class DeploymentList(PaginationMixin):
    """
    Paginated list of Deployment
    """

    data: List[DeploymentModel]


class DeploymentUpdate(BaseDocumentServiceUpdateSchema):
    """
    Schema for deployment update
    """

    enabled: Optional[bool]


class DeploymentSummary(FeatureByteBaseModel):
    """
    Schema for deployment summary
    """

    num_feature_list: int
    num_feature: int


class DeploymentInfo(BaseInfo):
    """
    Schema for deployment info
    """

    feature_list_name: str
    feature_list_version: str
    num_feature: int
    enabled: bool
