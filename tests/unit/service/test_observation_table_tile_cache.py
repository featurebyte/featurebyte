"""
Unit tests for ObservationTableTileCacheService
"""

import pytest
from bson import ObjectId


@pytest.fixture(name="service")
def service_fixture(app_container):
    """
    Fixture for ObservationTableTileCacheService
    """
    return app_container.observation_table_tile_cache_service


@pytest.fixture(name="observation_table_id")
def observation_table_id_fixture():
    """
    Fixture for observation_table_id
    """
    return ObjectId()


@pytest.fixture(name="another_observation_table_id")
def another_observation_table_id_fixture():
    """
    Fixture for another_observation_table_id
    """
    return ObjectId()


@pytest.mark.asyncio
async def test_add_aggregation_ids(service, observation_table_id, another_observation_table_id):
    """
    Test add_aggregation_ids_for_observation_table
    """
    await service.add_aggregation_ids_for_observation_table(
        observation_table_id, ["agg_id_1", "agg_id_2"]
    )

    # Check cached aggregation ids
    query_agg_ids = ["agg_id_1", "agg_id_2", "agg_id_3"]
    assert await service.get_non_cached_aggregation_ids(observation_table_id, query_agg_ids) == [
        "agg_id_3"
    ]

    # Check another observation table id which should not be affected
    assert (
        await service.get_non_cached_aggregation_ids(another_observation_table_id, query_agg_ids)
        == query_agg_ids
    )

    # Cache more aggregation ids
    await service.add_aggregation_ids_for_observation_table(observation_table_id, ["agg_id_3"])
    assert await service.get_non_cached_aggregation_ids(observation_table_id, query_agg_ids) == []
