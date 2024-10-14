"""
Pydantic schemas for handling API payloads for deployment routes
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.deployment import DeploymentModel, FeastRegistryInfo
from featurebyte.models.feature_materialize_run import CompletionStatus
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class DeploymentCreate(FeatureByteBaseModel):
    """
    Schema for deployment creation
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: Optional[NameStr] = Field(default=None)
    feature_list_id: PydanticObjectId
    use_case_id: Optional[PydanticObjectId] = Field(default=None)


class DeploymentList(PaginationMixin):
    """
    Paginated list of Deployment
    """

    data: List[DeploymentModel]


class DeploymentUpdate(BaseDocumentServiceUpdateSchema):
    """
    Schema for deployment update
    """

    enabled: Optional[bool] = Field(default=None)


class DeploymentServiceUpdate(DeploymentUpdate):
    """
    Schema for deployment service update
    """

    registry_info: Optional[FeastRegistryInfo] = Field(default=None)


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


class FeatureTableJobRun(FeatureByteBaseModel):
    """
    Schema for a specific job run
    """

    scheduled_ts: datetime
    completion_ts: Optional[datetime]
    completion_status: Optional[CompletionStatus]
    duration_seconds: Optional[float]
    incomplete_tile_tasks_count: Optional[int]


class FeatureTableJobRuns(FeatureByteBaseModel):
    """
    Schema for feature table job runs
    """

    feature_table_id: PydanticObjectId
    feature_table_name: Optional[str]
    runs: List[FeatureTableJobRun]


class DeploymentJobHistory(FeatureByteBaseModel):
    """
    Schema for deployment job history
    """

    feature_table_history: List[FeatureTableJobRuns]
