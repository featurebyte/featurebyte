"""
Tests for ColumnStatisticsService
"""

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.column_statistics import ColumnStatisticsModel, StatisticsModel


@pytest.fixture(name="service")
def service_fixture(app_container):
    """
    Fixture for ColumnStatisticsService
    """
    return app_container.column_statistics_service


@pytest.fixture(name="table_id")
def table_id_fixture():
    """
    Fixture for table_id
    """
    return ObjectId()


@pytest_asyncio.fixture(name="saved_column_statistics")
async def saved_column_statistics_fixture(service, table_id):
    """
    Fixture for saved column statistics
    """
    models = []
    for i in [1, 2, 3]:
        model = ColumnStatisticsModel(
            table_id=table_id,
            column_name=f"column_{i}",
            stats=StatisticsModel(
                distinct_count=100 * i,
            ),
        )
        model = await service.create_document(model)
        models.append(model)
    return models


@pytest.mark.asyncio
async def test_get_catalog_column_statistics(service, saved_column_statistics):
    """
    Test get_catalog_column_statistics
    """
    column_statistics = await service.get_catalog_column_statistics()
    assert sorted(column_statistics, key=lambda x: x.column_name) == saved_column_statistics
