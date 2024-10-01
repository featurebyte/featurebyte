"""
Unit tests for SystemMetricsService
"""

import pytest
from bson import ObjectId

from featurebyte.models.system_metrics import HistoricalFeaturesMetrics
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
def historical_feature_metrics_data(historical_feature_table_id):
    """
    Fixture for historical_feature_metrics_data
    """
    return HistoricalFeaturesMetrics(
        tile_compute_seconds=3600,
        feature_compute_seconds=7200,
        historical_feature_table_id=historical_feature_table_id,
    )


@pytest.mark.asyncio
async def test_create_metrics(
    service, historical_feature_metrics_data, historical_feature_table_id
):
    """
    Test create_metrics
    """
    await service.create_metrics(
        metrics_data=historical_feature_metrics_data,
    )
    query_filter = {"metrics_data.historical_feature_table_id": historical_feature_table_id}
    doc = None
    async for doc in service.list_documents_iterator(query_filter=query_filter):
        break
    assert doc is not None
    assert {
        "metrics_data": {
            "tile_compute_seconds": 3600,
            "feature_compute_seconds": 7200,
            "feature_cache_update_seconds": None,
            "historical_feature_table_id": historical_feature_table_id,
            "type": "historical_features",
        },
    }.items() < doc.dict().items()
