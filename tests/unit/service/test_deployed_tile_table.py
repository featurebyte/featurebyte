"""
Tests for DeployedTileTableService
"""

import pytest
import pytest_asyncio
from sqlglot import parse_one

from featurebyte.models.deployed_tile_table import DeployedTileTableModel, TileIdentifier
from featurebyte.models.tile_compute_query import QueryModel, TileComputeQuery


@pytest.fixture(name="service")
def service_fixture(app_container):
    """
    Fixture for DeployedTileTableService
    """
    return app_container.deployed_tile_table_service


@pytest_asyncio.fixture(name="deployed_tile_tables")
async def deployed_tile_tables_fixture(service, source_info):
    """
    Fixture for DeployedTileTableModel models
    """
    model_1 = DeployedTileTableModel(
        table_name="tile_table_1",
        tile_identifiers=[
            TileIdentifier(
                tile_id="tile_1",
                aggregation_id="aggregation_1",
            ),
            TileIdentifier(
                tile_id="tile_2",
                aggregation_id="aggregation_2",
            ),
        ],
        tile_compute_query=TileComputeQuery(
            aggregation_query=QueryModel.from_expr(
                parse_one("SELECT * FROM event_table"), source_info.source_type
            ),
        ),
        entity_column_names=["cust_id"],
        value_column_names=["aggregation_1_col", "aggregation_2_col"],
        value_column_types=["FLOAT", "FLOAT"],
        frequency_minute=60,
        time_modulo_frequency_second=120,
        blind_spot_second=600,
    )
    model_2 = DeployedTileTableModel(
        table_name="tile_table_2",
        tile_identifiers=[
            TileIdentifier(
                tile_id="tile_3",
                aggregation_id="aggregation_3",
            ),
        ],
        tile_compute_query=TileComputeQuery(
            aggregation_query=QueryModel.from_expr(
                parse_one("SELECT * FROM event_table"), source_info.source_type
            ),
        ),
        entity_column_names=["cust_id"],
        value_column_names=["aggregation_3_col"],
        value_column_types=["INT"],
        frequency_minute=60,
        time_modulo_frequency_second=120,
        blind_spot_second=600,
    )
    await service.create_document(model_1)
    await service.create_document(model_2)


@pytest.mark.asyncio
async def test_get_deployed_aggregation_ids(service, deployed_tile_tables):
    """
    Test get_deployed_aggregation_ids
    """
    _ = deployed_tile_tables
    deployed_aggregation_ids = await service.get_deployed_aggregation_ids({"aggregation_1"})
    assert deployed_aggregation_ids == {"aggregation_1"}

    deployed_aggregation_ids = await service.get_deployed_aggregation_ids({
        "aggregation_1",
        "aggregation_3",
    })
    assert deployed_aggregation_ids == {"aggregation_1", "aggregation_3"}

    deployed_aggregation_ids = await service.get_deployed_aggregation_ids({"new_aggregation"})
    assert deployed_aggregation_ids == set()
