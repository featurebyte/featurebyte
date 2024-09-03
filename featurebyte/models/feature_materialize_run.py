"""
FeatureMaterializeRunModel class
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field
from pymongo import IndexModel

from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema

IncompleteTileTaskReason = Literal["failure", "timeout"]
CompletionStatus = Literal["success", "failure"]


class IncompleteTileTask(FeatureByteBaseModel):
    """
    Represents an incomplete tile task that a feature materialize task depended upon
    """

    aggregation_id: str
    reason: IncompleteTileTaskReason


class FeatureMaterializeRun(FeatureByteCatalogBaseDocumentModel):
    """
    Represents a feature materialize job run
    """

    offline_store_feature_table_id: PydanticObjectId
    offline_store_feature_table_name: str | None = Field(default=None)
    scheduled_job_ts: datetime
    feature_materialize_ts: datetime | None = Field(default=None)
    completion_ts: datetime | None = Field(default=None)
    completion_status: CompletionStatus | None = Field(default=None)
    duration_from_scheduled_seconds: float | None = Field(default=None)
    incomplete_tile_tasks: list[IncompleteTileTask] | None = Field(default=None)
    deployment_ids: list[PydanticObjectId] | None = Field(default=None)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature_materialize_run"
        unique_constraints: list[UniqueValuesConstraint] = []
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            IndexModel("offline_store_feature_table_id"),
            IndexModel("scheduled_job_ts"),
            IndexModel("deployment_ids"),
        ]
        auditable = False


class FeatureMaterializeRunUpdate(BaseDocumentServiceUpdateSchema):
    """
    FeatureMaterializeRun update schema
    """

    feature_materialize_ts: datetime | None = Field(default=None)
    completion_ts: datetime | None = Field(default=None)
    completion_status: CompletionStatus | None = Field(default=None)
    duration_from_scheduled_seconds: float | None = Field(default=None)
    incomplete_tile_tasks: list[IncompleteTileTask] | None = Field(default=None)
