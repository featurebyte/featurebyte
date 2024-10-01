"""
Unit tests for SystemMetricsService
"""

import pytest
from bson import ObjectId

from featurebyte.models.system_metrics import HistoricalFeaturesMetrics, SystemMetricsMetadataKey
from featurebyte.service.system_metrics import SystemMetricsService


@pytest.fixture
def service(app_container) -> SystemMetricsService:
    """
    Fixture for SystemMetricsService
    """
    return app_container.system_metrics_service


@pytest.fixture
def historical_feature_table_id():
    """
    Fixture for historical_feature_table_id
    """
    return ObjectId()


@pytest.fixture
def metrics_metadata(historical_feature_table_id):
    """
    Fixture for metrics_metadata
    """
    return {
        SystemMetricsMetadataKey.historical_feature_table_id: historical_feature_table_id,
    }


@pytest.mark.asyncio
async def test_create_metrics(service, metrics_metadata):
    """
    Test create_metrics
    """
    await service.create_metrics(
        metadata=metrics_metadata,
        metrics=HistoricalFeaturesMetrics(tile_compute_seconds=3600, feature_compute_seconds=7200),
    )
    metrics = await service.get_metrics(metadata=metrics_metadata)
    assert {
        "metadata": metrics_metadata,
        "metrics": {
            "tile_compute_seconds": 3600,
            "feature_compute_seconds": 7200,
            "feature_cache_update_seconds": None,
            "type": "historical_features",
        },
    }.items() < metrics.dict().items()
