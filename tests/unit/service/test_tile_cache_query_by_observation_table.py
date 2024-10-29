"""
Tests for TileCacheQueryByObservationTableService
"""

from unittest.mock import patch

import pytest
from bson import ObjectId

from featurebyte.service.tile_cache_query_by_observation_table import (
    TileCacheQueryByObservationTableService,
)
from featurebyte.service.tile_manager import TileManagerService
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(autouse=True)
def patched_unique_identifier():
    """
    Patch ObjectId to return a fixed value
    """
    with patch(
        "featurebyte.service.tile_cache_query_by_observation_table.ObjectId"
    ) as mock_object_id:
        mock_object_id.return_value = ObjectId("0" * 24)
        yield


@pytest.fixture(name="service")
def service_fixture(app_container) -> TileCacheQueryByObservationTableService:
    """
    Fixture for TileCacheQueryByObservationTableService
    """
    return app_container.tile_cache_query_by_observation_table_service


@pytest.fixture(name="tile_manager_service")
def tile_manager_service_fixture(app_container) -> TileManagerService:
    """
    Fixture for TileManagerService
    """
    return app_container.tile_manager_service


@pytest.fixture(name="observation_table_id")
def observation_table_id_fixture():
    """
    Fixture for observation_table_id
    """
    return ObjectId()


@pytest.mark.asyncio
async def test_get_required_computation(
    service,
    tile_manager_service,
    mock_snowflake_session,
    feature_store,
    production_ready_feature,
    observation_table_id,
    update_fixtures,
):
    """
    Test get_required_computation
    """

    async def _get_required_computation():
        return await service.get_required_computation(
            session=mock_snowflake_session,
            feature_store=feature_store,
            request_id="1234",
            graph=production_ready_feature.graph,
            nodes=[
                production_ready_feature.graph.get_node_by_name(production_ready_feature.node_name)
            ],
            request_table_name="my_request_table",
            observation_table_id=observation_table_id,
        )

    request_set = await _get_required_computation()

    # Check there should be one compute request
    compute_requests = request_set.compute_requests
    assert len(compute_requests) == 1
    request = compute_requests[0]
    assert request.observation_table_id == observation_table_id
    assert request.tracker_sql is None
    assert request_set.materialized_temp_table_names == {
        "ON_DEMAND_TILE_ENTITY_TABLE_000000000000000000000000"
    }

    # Run on demand tile generation and check again
    tile_manager_inputs = [
        request.to_tile_manager_input(feature_store.id) for request in compute_requests
    ]
    await tile_manager_service.generate_tiles_on_demand(mock_snowflake_session, tile_manager_inputs)
    request_set = await _get_required_computation()
    assert len(request_set.compute_requests) == 0
    assert request_set.materialized_temp_table_names == set()

    # Check the executed queries
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/tile_cache_query_by_observation_table/expected_queries.sql",
        update_fixtures,
    )
