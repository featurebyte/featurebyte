"""
Pydantic schemas for handling API payloads for deployment routes
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

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

    id: PydanticObjectId | None = Field(default_factory=ObjectId, alias="_id")
    name: NameStr | None = Field(default=None)
    feature_list_id: PydanticObjectId
    use_case_id: PydanticObjectId | None = Field(default=None)


class DeploymentList(PaginationMixin):
    """
    Paginated list of Deployment
    """

    data: list[DeploymentModel]


class DeploymentUpdate(BaseDocumentServiceUpdateSchema):
    """
    Schema for deployment update
    """

    enabled: bool | None = Field(default=None)


class DeploymentServiceUpdate(DeploymentUpdate):
    """
    Schema for deployment service update
    """

    registry_info: FeastRegistryInfo | None = Field(default=None)


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

    data: list[AllDeploymentListRecord]


class OnlineFeaturesResponseModel(FeatureByteBaseModel):
    """
    Response model for online features
    """

    features: list[dict[str, Any]]


class FeatureTableJobRun(FeatureByteBaseModel):
    """
    Schema for a specific job run
    """

    scheduled_ts: datetime
    completion_ts: datetime | None
    completion_status: CompletionStatus | None
    duration_seconds: int | None
    incomplete_tile_tasks_count: int | None


class FeatureTableJobRuns(FeatureByteBaseModel):
    """
    Schema for feature table job runs
    """

    feature_table_id: PydanticObjectId
    feature_table_name: str | None
    runs: list[FeatureTableJobRun]


class DeploymentJobHistory(FeatureByteBaseModel):
    """
    Schema for deployment job history
    """

    feature_table_history: list[FeatureTableJobRuns]
