"""
This module contains integration tests for TileSnowflake
"""
import pytest

from featurebyte.enum import InternalName
from featurebyte.models.tile import TileType


@pytest.mark.asyncio
async def test_generate_tiles(snowflake_tile, spark_session, spark_tile_manager):
    """
    Test generate_tiles method in TileSnowflake
    """

    await spark_tile_manager.generate_tiles(
        snowflake_tile,
        TileType.ONLINE,
        "2022-06-05 23:33:00",
        "2022-06-05 23:58:00",
        "2022-06-05 23:53:00",
    )

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {snowflake_tile.tile_id}"
    result = await spark_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5


@pytest.mark.asyncio
async def test_update_tile_entity_tracker(snowflake_tile, spark_session, spark_tile_manager):
    """
    Test update_tile_entity_tracker method in TileSnowflake
    """

    temp_entity_table = "TEMP_ENTITY_TRACKER_2"
    last_tile_start_date_1 = "2022-07-06 10:52:14"
    last_tile_start_date_2 = "2022-07-07 10:52:14"

    await spark_session.execute_query(
        f"CREATE TABLE {temp_entity_table} (PRODUCT_ACTION STRING, CUST_ID STRING, LAST_TILE_START_DATE STRING) USING delta"
    )
    await spark_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P1', 'C1', '{last_tile_start_date_1}') "
    )
    await spark_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P2', 'C2', '{last_tile_start_date_2}') "
    )

    await spark_tile_manager.update_tile_entity_tracker(
        tile_spec=snowflake_tile, temp_entity_table=temp_entity_table
    )

    sql = f"SELECT * FROM {snowflake_tile.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await spark_session.execute_query(sql)
    assert len(result) == 2
    assert result["PRODUCT_ACTION"].iloc[0] == "P1"
    assert result["CUST_ID"].iloc[0] == "C1"
    assert result["LAST_TILE_START_DATE"].iloc[0] == last_tile_start_date_1

    assert result["PRODUCT_ACTION"].iloc[1] == "P2"
    assert result["CUST_ID"].iloc[1] == "C2"
    assert result["LAST_TILE_START_DATE"].iloc[1] == last_tile_start_date_2

    last_tile_start_date_2_new = "2022-07-08 00:00:00"
    await spark_session.execute_query(
        f"UPDATE {temp_entity_table} SET LAST_TILE_START_DATE = '{last_tile_start_date_2_new}' WHERE PRODUCT_ACTION = 'P2'"
    )
    last_tile_start_date_3 = "2022-07-08 10:52:14"
    await spark_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P3', 'C3', '{last_tile_start_date_3}') "
    )

    await spark_tile_manager.update_tile_entity_tracker(
        tile_spec=snowflake_tile, temp_entity_table=temp_entity_table
    )

    sql = f"SELECT * FROM {snowflake_tile.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await spark_session.execute_query(sql)
    assert len(result) == 3
    assert result["PRODUCT_ACTION"].iloc[1] == "P2"
    assert result["CUST_ID"].iloc[1] == "C2"
    assert result["LAST_TILE_START_DATE"].iloc[1] == last_tile_start_date_2_new

    assert result["PRODUCT_ACTION"].iloc[2] == "P3"
    assert result["CUST_ID"].iloc[2] == "C3"
    assert result["LAST_TILE_START_DATE"].iloc[2] == last_tile_start_date_3


@pytest.mark.asyncio
async def test_generate_tiles_on_demand(spark_session, snowflake_tile, spark_tile_manager):
    """
    Test generate_tiles_on_demand
    """
    temp_entity_table = "TEMP_ENTITY_TRACKER_1"
    last_tile_start_date_1 = "2022-07-06 10:52:14"

    await spark_session.execute_query(
        f"CREATE TABLE {temp_entity_table} (PRODUCT_ACTION STRING, CUST_ID STRING, LAST_TILE_START_DATE STRING)"
    )
    await spark_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P1', 'C1', '{last_tile_start_date_1}') "
    )

    snowflake_tile.tile_sql = snowflake_tile.tile_sql.replace(
        InternalName.TILE_START_DATE_SQL_PLACEHOLDER, "'2022-06-05 23:33:00'"
    ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, "'2022-06-05 23:58:00'")

    await spark_tile_manager.generate_tiles_on_demand([(snowflake_tile, temp_entity_table)])

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {snowflake_tile.tile_id}"
    result = await spark_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5

    sql = f"SELECT * FROM {snowflake_tile.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await spark_session.execute_query(sql)
    assert len(result) == 1
    assert result["PRODUCT_ACTION"].iloc[0] == "P1"
    assert result["CUST_ID"].iloc[0] == "C1"
    assert result["LAST_TILE_START_DATE"].iloc[0] == last_tile_start_date_1
