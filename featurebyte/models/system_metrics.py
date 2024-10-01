"""
SystemMetricsModel class
"""

from __future__ import annotations

from typing import Annotated, Literal, Optional, Union

from pydantic import Field
from pymongo import IndexModel

from featurebyte.enum import StrEnum
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
)


class SystemMetricsType(StrEnum):
    """
    SystemMetricsType class
    """

    HISTORICAL_FEATURES = "historical_features"
    TILE_TASK = "tile_task"


class HistoricalFeaturesMetrics(FeatureByteBaseModel):
    """
    HistoricalFeaturesMetrics class
    """

    historical_feature_table_id: Optional[PydanticObjectId] = None
    tile_compute_seconds: Optional[float] = None
    feature_compute_seconds: Optional[float] = None
    feature_cache_update_seconds: Optional[float] = None
    type: Literal[SystemMetricsType.HISTORICAL_FEATURES] = SystemMetricsType.HISTORICAL_FEATURES


class TileTaskMetrics(FeatureByteBaseModel):
    """
    TileTaskMetrics class
    """

    tile_compute_seconds: Optional[float] = None
    internal_online_compute_seconds: Optional[float] = None
    type: Literal[SystemMetricsType.TILE_TASK] = SystemMetricsType.TILE_TASK


SystemMetricsData = Annotated[
    Union[HistoricalFeaturesMetrics, TileTaskMetrics], Field(discriminator="type")
]


class SystemMetricsModel(FeatureByteCatalogBaseDocumentModel):
    """
    SystemMetricsModel class
    """

    metrics_data: SystemMetricsData

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        collection_name: str = "system_metrics"
        unique_constraints = []
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            IndexModel("metrics_data.type"),
            IndexModel("metrics_data.historical_feature_table_id"),
        ]
        auditable = False
