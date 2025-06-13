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
    BATCH_FEATURES = "batch_features"
    TILE_TASK = "tile_task"
    SCHEDULED_FEATURE_MATERIALIZE = "scheduled_feature_materialize"
    DEPLOYMENT_ENABLEMENT = "deployment_enablement"
    SQL_QUERY = "sql_query"


class TileComputeMetrics(FeatureByteBaseModel):
    """
    TileGenerationMetrics class
    """

    view_cache_seconds: Optional[float] = None
    compute_seconds: Optional[float] = None


class HistoricalFeaturesMetrics(FeatureByteBaseModel):
    """
    HistoricalFeaturesMetrics class
    """

    historical_feature_table_id: Optional[PydanticObjectId] = None
    tile_compute_seconds: Optional[float] = None
    tile_compute_metrics: Optional[TileComputeMetrics] = None
    feature_compute_seconds: Optional[float] = None
    feature_cache_update_seconds: Optional[float] = None
    total_seconds: Optional[float] = None
    metrics_type: Literal[SystemMetricsType.HISTORICAL_FEATURES] = (
        SystemMetricsType.HISTORICAL_FEATURES
    )


class BatchFeaturesMetrics(FeatureByteBaseModel):
    """
    BatchFeaturesMetrics class
    """

    batch_feature_table_id: Optional[PydanticObjectId] = None
    total_seconds: Optional[float] = None
    metrics_type: Literal[SystemMetricsType.BATCH_FEATURES] = SystemMetricsType.BATCH_FEATURES


class TileTaskMetrics(FeatureByteBaseModel):
    """
    TileTaskMetrics class
    """

    tile_table_id: str
    tile_monitor_seconds: Optional[float] = None
    tile_compute_seconds: Optional[float] = None
    internal_online_compute_seconds: Optional[float] = None
    metrics_type: Literal[SystemMetricsType.TILE_TASK] = SystemMetricsType.TILE_TASK


class ScheduledFeatureMaterializeMetrics(FeatureByteBaseModel):
    """
    ScheduledFeatureMaterializeMetrics class
    """

    offline_store_feature_table_id: PydanticObjectId
    num_columns: int
    generate_entity_universe_seconds: Optional[float] = None
    generate_feature_table_seconds: Optional[float] = None
    generate_precomputed_lookup_feature_tables_seconds: Optional[float] = None
    update_feature_tables_seconds: Optional[float] = None
    online_materialize_seconds: Optional[float] = None
    total_seconds: Optional[float] = None
    metrics_type: Literal[SystemMetricsType.SCHEDULED_FEATURE_MATERIALIZE] = (
        SystemMetricsType.SCHEDULED_FEATURE_MATERIALIZE
    )


class DeploymentEnablementMetrics(FeatureByteBaseModel):
    """
    DeploymentEnablementMetrics class
    """

    deployment_id: PydanticObjectId
    tile_tables_seconds: Optional[float] = None
    offline_feature_tables_seconds: Optional[float] = None
    precompute_lookup_feature_tables_seconds: Optional[float] = None
    total_seconds: Optional[float] = None
    metrics_type: Literal[SystemMetricsType.DEPLOYMENT_ENABLEMENT] = (
        SystemMetricsType.DEPLOYMENT_ENABLEMENT
    )


class SqlQueryType(StrEnum):
    """
    SqlQueryType class
    """

    TILE_COMPUTE = "TILE_COMPUTE"
    FEATURE_COMPUTE = "FEATURE_COMPUTE"


class SqlQueryMetrics(FeatureByteBaseModel):
    """
    SqlQueryMetrics class
    """

    query: str
    total_seconds: float
    query_type: SqlQueryType
    query_id: Optional[str] = None
    feature_names: Optional[list[str]] = None
    observation_table_id: Optional[PydanticObjectId] = None
    batch_request_table_id: Optional[PydanticObjectId] = None
    metrics_type: Literal[SystemMetricsType.SQL_QUERY] = SystemMetricsType.SQL_QUERY


SystemMetricsData = Annotated[
    Union[
        HistoricalFeaturesMetrics,
        BatchFeaturesMetrics,
        TileTaskMetrics,
        ScheduledFeatureMaterializeMetrics,
        DeploymentEnablementMetrics,
        SqlQueryMetrics,
    ],
    Field(discriminator="metrics_type"),
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
            IndexModel("metrics_data.metrics_type"),
            IndexModel("metrics_data.historical_feature_table_id"),
            IndexModel("metrics_data.tile_table_id"),
            IndexModel("metrics_data.offline_store_feature_table_id"),
            IndexModel("metrics_data.deployment_id"),
            IndexModel("metrics_data.observation_table_id"),
            IndexModel("metrics_data.batch_feature_table_id"),
        ]
        auditable = False
