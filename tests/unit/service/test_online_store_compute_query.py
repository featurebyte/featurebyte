"""
Tests for OnlineStoreComputeQueryService
"""

import pytest
import pytest_asyncio

from featurebyte.models.online_store_compute_query import OnlineStoreComputeQueryModel
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService


@pytest.fixture(name="service")
def service_fixture(app_container) -> OnlineStoreComputeQueryService:
    """
    OnlineStoreComputeQueryService object fixture
    """
    return app_container.online_store_compute_query_service


@pytest_asyncio.fixture(name="saved_online_store_compute_query")
async def saved_online_store_compute_query_fixture(service, snowflake_feature_store_id):
    """
    Fixture for saved online store compute query
    """
    model = OnlineStoreComputeQueryModel(
        tile_id="tile_1",
        aggregation_id="agg_1",
        result_name="result_1",
        result_type="FLOAT",
        sql="SELECT * FROM tile_1",
        table_name="table_1",
        serving_names=["cust_id"],
        feature_store_id=snowflake_feature_store_id,
    )
    saved_model = await service.create_document(model)
    yield saved_model


@pytest_asyncio.fixture(name="saved_online_store_compute_query_use_deployed_tile_table")
async def saved_online_store_compute_query_use_deployed_tile_table_fixture(
    service, snowflake_feature_store_id
):
    """
    Fixture for saved online store compute query with use_deployed_tile_table set to False
    """
    model = OnlineStoreComputeQueryModel(
        tile_id="tile_1",
        aggregation_id="agg_1",
        result_name="result_1",
        result_type="FLOAT",
        serving_names=["cust_id"],
        sql="SELECT * FROM deployed_tile_table_001",
        table_name="table_1",
        feature_store_id=snowflake_feature_store_id,
        use_deployed_tile_table=True,
    )
    saved_model = await service.create_document(model)
    yield saved_model


@pytest.fixture()
async def saved_models(
    saved_online_store_compute_query,
    saved_online_store_compute_query_use_deployed_tile_table,
):
    """
    Fixture for saved online store compute queries
    """
    return [
        saved_online_store_compute_query,
        saved_online_store_compute_query_use_deployed_tile_table,
    ]


@pytest.mark.parametrize(
    "use_deployed_tile_table, expected_sql",
    [
        (False, "SELECT * FROM tile_1"),
        (True, "SELECT * FROM deployed_tile_table_001"),
    ],
)
@pytest.mark.usefixtures("saved_models")
@pytest.mark.asyncio
async def test_list_by_result_names(service, use_deployed_tile_table, expected_sql):
    """
    Test listing online store compute queries by result names
    """
    models = [
        doc
        async for doc in service.list_by_result_names(
            ["result_1"], use_deployed_tile_table=use_deployed_tile_table
        )
    ]
    assert len(models) == 1
    model = models[0]
    assert model.result_name == "result_1"
    assert model.sql == expected_sql


@pytest.mark.parametrize(
    "use_deployed_tile_table, expected_sql",
    [
        (False, "SELECT * FROM tile_1"),
        (True, "SELECT * FROM deployed_tile_table_001"),
    ],
)
@pytest.mark.usefixtures("saved_models")
@pytest.mark.asyncio
async def test_list_by_aggregation_ids(service, use_deployed_tile_table, expected_sql):
    """
    Test listing online store compute queries by aggregation ids
    """
    models = [
        doc
        async for doc in service.list_by_aggregation_ids(
            ["agg_1"], use_deployed_tile_table=use_deployed_tile_table
        )
    ]
    assert len(models) == 1
    model = models[0]
    assert model.result_name == "result_1"
    assert model.sql == expected_sql
