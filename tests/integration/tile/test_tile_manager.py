"""
This module contains integration tests for TileManager
"""

import pytest

from featurebyte.enum import InternalName
from featurebyte.models.tile import OnDemandTileSpec, TileType
from featurebyte.query_graph.sql.tile_compute_combine import TileTableGrouping


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tiles(tile_spec, session, tile_manager_service):
    """
    Test generate_tiles method in TileSnowflake
    """

    await tile_manager_service.generate_tiles(
        session,
        tile_spec,
        TileType.ONLINE,
        "2022-06-05 23:33:00",
        "2022-06-05 23:58:00",
        deployed_tile_table_id=None,
    )

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_spec.tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tiles_on_demand(session, tile_spec, tile_manager_service):
    """
    Test generate_tiles_on_demand
    """
    temp_entity_table = "TEMP_ENTITY_TRACKER_1"
    last_tile_start_date_1 = "2022-07-06 10:52:14"

    await session.execute_query(
        f"CREATE TABLE {temp_entity_table} (PRODUCT_ACTION STRING, CUST_ID STRING, LAST_TILE_START_DATE STRING)"
    )
    await session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P1', 'C1', '{last_tile_start_date_1}') "
    )

    tile_spec.tile_sql = tile_spec.tile_sql.replace(
        InternalName.TILE_START_DATE_SQL_PLACEHOLDER, "'2022-06-05 23:33:00'"
    ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, "'2022-06-05 23:58:00'")

    result = await tile_manager_service.generate_tiles_on_demand(
        session,
        [
            OnDemandTileSpec(
                tile_spec=tile_spec,
                tile_table_groupings=[
                    TileTableGrouping(
                        value_column_names=tile_spec.value_column_names,
                        value_column_types=tile_spec.value_column_types,
                        tile_id=tile_spec.tile_id,
                        aggregation_id=tile_spec.aggregation_id,
                    )
                ],
            )
        ],
        "some_tag",
    )

    # check on demand tile table is created
    assert len(result.on_demand_tile_tables) == 1
    temp_tile_table_name = result.on_demand_tile_tables[0].on_demand_table_name
    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {temp_tile_table_name}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5
