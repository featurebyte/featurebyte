"""
Pydantic schemas for handling API payloads for deployment routes
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.deployment import DeploymentModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


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


class AllDeploymentListRecord(FeatureByteBaseModel):
    """
    Schema for all deployment list record
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    name: str
    catalog_name: str
    feature_list_name: str
    feature_list_version: str
    num_feature: int


class AllDeploymentList(PaginationMixin):
    """
    Paginated list of Deployment
    """

    data: List[AllDeploymentListRecord]


class OnlineFeaturesResponseModel(FeatureByteBaseModel):
    """
    Response model for online features
    """

    features: List[Dict[str, Any]]
