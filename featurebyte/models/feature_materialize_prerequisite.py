"""
FeatureMaterializePrerequisiteModel
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Literal

from pydantic import Field
from pymongo import IndexModel

from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
)

PrerequisiteTileTaskStatusType = Literal["success", "failure"]

CACHE_DAYS = 7


class PrerequisiteTileTask(FeatureByteBaseModel):
    """
    Represents a completed tile task that a feature materialize task depended upon
    """

    aggregation_id: str
    status: PrerequisiteTileTaskStatusType


class FeatureMaterializePrerequisite(FeatureByteCatalogBaseDocumentModel):
    """
    Represents a set of prerequisites that have to be met before a feature materialize task for an
    offline store feature table can run
    """

    offline_store_feature_table_id: PydanticObjectId
    scheduled_job_ts: datetime
    completed: List[PrerequisiteTileTask] = Field(default_factory=list)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature_materialize_prerequisite"
        unique_constraints = []
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            IndexModel("offline_store_feature_table_id"),
            IndexModel("scheduled_job_ts", expireAfterSeconds=CACHE_DAYS * 86400),
        ]
        auditable = False
