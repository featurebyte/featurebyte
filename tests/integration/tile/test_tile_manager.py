"""
This module contains integration tests for TileSnowflake
"""
import pytest

from featurebyte.enum import InternalName
from featurebyte.models.tile import TileType


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tiles(tile_spec, session, tile_manager):
    """
    Test generate_tiles method in TileSnowflake
    """

    await tile_manager.generate_tiles(
        tile_spec,
        TileType.ONLINE,
        "2022-06-05 23:33:00",
        "2022-06-05 23:58:00",
        "2022-06-05 23:53:00",
    )

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_spec.tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_update_tile_entity_tracker(tile_spec, session, tile_manager, base_sql_model):
    """
    Test update_tile_entity_tracker method in TileSnowflake
    """

    temp_entity_table = "TEMP_ENTITY_TRACKER_2"
    entity_table_query = "SELECT * FROM TEMP_ENTITY_TRACKER_2"
    last_tile_start_date_1 = "2022-07-06 10:52:14"
    last_tile_start_date_2 = "2022-07-07 10:52:14"

    await session.execute_query(
        base_sql_model.sql_table_with_delta(
            f"CREATE TABLE {temp_entity_table} (PRODUCT_ACTION STRING, CUST_ID STRING, LAST_TILE_START_DATE STRING) {base_sql_model.delta_placeholder}"
        )
    )
    await session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P1', 'C1', '{last_tile_start_date_1}') "
    )
    await session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P2', 'C2', '{last_tile_start_date_2}') "
    )

    await tile_manager.update_tile_entity_tracker(
        tile_spec=tile_spec, temp_entity_table=entity_table_query
    )

    sql = f"SELECT * FROM {tile_spec.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await session.execute_query(sql)
    assert len(result) == 2
    assert result["PRODUCT_ACTION"].iloc[0] == "P1"
    assert result["CUST_ID"].iloc[0] == "C1"
    assert result["LAST_TILE_START_DATE"].iloc[0] == last_tile_start_date_1

    assert result["PRODUCT_ACTION"].iloc[1] == "P2"
    assert result["CUST_ID"].iloc[1] == "C2"
    assert result["LAST_TILE_START_DATE"].iloc[1] == last_tile_start_date_2

    last_tile_start_date_2_new = "2022-07-08 00:00:00"
    await session.execute_query(
        f"UPDATE {temp_entity_table} SET LAST_TILE_START_DATE = '{last_tile_start_date_2_new}' WHERE PRODUCT_ACTION = 'P2'"
    )
    last_tile_start_date_3 = "2022-07-08 10:52:14"
    await session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P3', 'C3', '{last_tile_start_date_3}') "
    )

    await tile_manager.update_tile_entity_tracker(
        tile_spec=tile_spec, temp_entity_table=entity_table_query
    )

    sql = f"SELECT * FROM {tile_spec.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await session.execute_query(sql)
    assert len(result) == 3
    assert result["PRODUCT_ACTION"].iloc[1] == "P2"
    assert result["CUST_ID"].iloc[1] == "C2"
    assert result["LAST_TILE_START_DATE"].iloc[1] == last_tile_start_date_2_new

    assert result["PRODUCT_ACTION"].iloc[2] == "P3"
    assert result["CUST_ID"].iloc[2] == "C3"
    assert result["LAST_TILE_START_DATE"].iloc[2] == last_tile_start_date_3


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tiles_on_demand(session, tile_spec, tile_manager):
    """
    Test generate_tiles_on_demand
    """
    temp_entity_table = "TEMP_ENTITY_TRACKER_1"
    entity_table_query = f"SELECT * FROM {temp_entity_table}"
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

    await tile_manager.generate_tiles_on_demand([(tile_spec, entity_table_query)])

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_spec.tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5

    sql = f"SELECT * FROM {tile_spec.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await session.execute_query(sql)
    assert len(result) == 1
    assert result["PRODUCT_ACTION"].iloc[0] == "P1"
    assert result["CUST_ID"].iloc[0] == "C1"
    assert result["LAST_TILE_START_DATE"].iloc[0] == last_tile_start_date_1
