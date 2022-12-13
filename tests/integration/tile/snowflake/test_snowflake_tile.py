"""
This module contains integration tests for TileSnowflake
"""
import pytest

from featurebyte.enum import InternalName
from featurebyte.models.tile import TileType


@pytest.mark.asyncio
async def test_generate_tile(snowflake_tile, snowflake_session, tile_manager):
    """
    Test generate_tiles method in TileSnowflake
    """
    await tile_manager.generate_tiles(
        snowflake_tile,
        TileType.ONLINE,
        "2022-06-05 23:33:00",
        "2022-06-05 23:58:00",
        "2022-06-05 23:53:00",
    )

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {snowflake_tile.tile_id}"
    result = await snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5


@pytest.mark.asyncio
async def test_schedule_online_tile(snowflake_tile, snowflake_session, tile_manager):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    minute_offset = snowflake_tile.time_modulo_frequency_second // 60

    await tile_manager.schedule_online_tiles(tile_spec=snowflake_tile)

    task_name = f"SHELL_TASK_{snowflake_tile.tile_id}_ONLINE".upper()

    result = await snowflake_session.execute_query(f"SHOW TASKS LIKE '%{snowflake_tile.tile_id}%'")
    assert len(result) == 1
    assert result["name"].iloc[0] == task_name
    assert result["schedule"].iloc[0] == f"USING CRON {minute_offset} * * * * UTC"
    assert result["state"].iloc[0] == "started"


@pytest.mark.asyncio
async def test_schedule_offline_tile(snowflake_tile, snowflake_session, tile_manager):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    await tile_manager.schedule_offline_tiles(tile_spec=snowflake_tile)

    task_name = f"SHELL_TASK_{snowflake_tile.tile_id}_OFFLINE".upper()

    result = await snowflake_session.execute_query(f"SHOW TASKS LIKE '%{snowflake_tile.tile_id}%'")
    assert len(result) == 1
    assert result["name"].iloc[0] == task_name
    assert result["schedule"].iloc[0] == "USING CRON 3 0 * * * UTC"
    assert result["state"].iloc[0] == "started"


@pytest.mark.asyncio
async def test_insert_tile_registry(snowflake_tile, snowflake_session, tile_manager):
    """
    Test insert_tile_registry method in TileSnowflake
    """
    flag = await tile_manager.insert_tile_registry(tile_spec=snowflake_tile)
    assert flag is True

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert bool(result["IS_ENABLED"].iloc[0]) is True
    assert (
        result["TIME_MODULO_FREQUENCY_SECOND"].iloc[0]
        == snowflake_tile.time_modulo_frequency_second
    )

    flag = await tile_manager.insert_tile_registry(tile_spec=snowflake_tile)
    assert flag is False

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert bool(result["IS_ENABLED"].iloc[0]) is True
    assert (
        result["TIME_MODULO_FREQUENCY_SECOND"].iloc[0]
        == snowflake_tile.time_modulo_frequency_second
    )


@pytest.mark.asyncio
async def test_disable_tiles(snowflake_tile, snowflake_session, tile_manager):
    """
    Test disable_tiles method in TileSnowflake
    """
    flag = await tile_manager.insert_tile_registry(tile_spec=snowflake_tile)
    assert flag is True

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert bool(result["IS_ENABLED"].iloc[0]) is True
    assert (
        result["TIME_MODULO_FREQUENCY_SECOND"].iloc[0]
        == snowflake_tile.time_modulo_frequency_second
    )

    # disable tile jobs
    await tile_manager.disable_tiles(tile_spec=snowflake_tile)

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert bool(result["IS_ENABLED"].iloc[0]) is False


@pytest.mark.asyncio
async def test_update_tile_entity_tracker(snowflake_tile, snowflake_session, tile_manager):
    """
    Test update_tile_entity_tracker method in TileSnowflake
    """

    temp_entity_table = "TEMP_ENTITY_TRACKER_2"
    last_tile_start_date_1 = "2022-07-06 10:52:14"
    last_tile_start_date_2 = "2022-07-07 10:52:14"

    await snowflake_session.execute_query(
        f"CREATE TEMPORARY TABLE {temp_entity_table} (PRODUCT_ACTION VARCHAR, CUST_ID VARCHAR, LAST_TILE_START_DATE TIMESTAMP_TZ)"
    )
    await snowflake_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P1', 'C1', '{last_tile_start_date_1}') "
    )
    await snowflake_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P2', 'C2', '{last_tile_start_date_2}') "
    )

    await tile_manager.update_tile_entity_tracker(
        tile_spec=snowflake_tile, temp_entity_table=temp_entity_table
    )

    sql = f"SELECT * FROM {snowflake_tile.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 2
    assert result["PRODUCT_ACTION"].iloc[0] == "P1"
    assert result["CUST_ID"].iloc[0] == "C1"
    assert (
        result["LAST_TILE_START_DATE"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        == last_tile_start_date_1
    )
    assert result["PRODUCT_ACTION"].iloc[1] == "P2"
    assert result["CUST_ID"].iloc[1] == "C2"
    assert (
        result["LAST_TILE_START_DATE"].iloc[1].strftime("%Y-%m-%d %H:%M:%S")
        == last_tile_start_date_2
    )

    last_tile_start_date_2_new = "2022-07-08 00:00:00"
    await snowflake_session.execute_query(
        f"UPDATE {temp_entity_table} SET LAST_TILE_START_DATE = '{last_tile_start_date_2_new}' WHERE PRODUCT_ACTION = 'P2'"
    )
    last_tile_start_date_3 = "2022-07-08 10:52:14"
    await snowflake_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P3', 'C3', '{last_tile_start_date_3}') "
    )

    await tile_manager.update_tile_entity_tracker(
        tile_spec=snowflake_tile, temp_entity_table=temp_entity_table
    )

    sql = f"SELECT * FROM {snowflake_tile.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 3
    assert result["PRODUCT_ACTION"].iloc[1] == "P2"
    assert result["CUST_ID"].iloc[1] == "C2"
    assert (
        result["LAST_TILE_START_DATE"].iloc[1].strftime("%Y-%m-%d %H:%M:%S")
        == last_tile_start_date_2_new
    )
    assert result["PRODUCT_ACTION"].iloc[2] == "P3"
    assert result["CUST_ID"].iloc[2] == "C3"
    assert (
        result["LAST_TILE_START_DATE"].iloc[2].strftime("%Y-%m-%d %H:%M:%S")
        == last_tile_start_date_3
    )


@pytest.mark.asyncio
async def test_generate_tiles_on_demand(snowflake_session, snowflake_tile, tile_manager):
    """
    Test generate_tiles_on_demand
    """
    temp_entity_table = "TEMP_ENTITY_TRACKER_1"
    last_tile_start_date_1 = "2022-07-06 10:52:14"
    await snowflake_session.execute_query(
        f"CREATE TEMPORARY TABLE {temp_entity_table} (PRODUCT_ACTION VARCHAR, CUST_ID VARCHAR, LAST_TILE_START_DATE TIMESTAMP_TZ)"
    )
    await snowflake_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P1', 'C1', '{last_tile_start_date_1}') "
    )

    snowflake_tile.tile_sql = snowflake_tile.tile_sql.replace(
        InternalName.TILE_START_DATE_SQL_PLACEHOLDER, "'2022-06-05 23:33:00'"
    ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, "'2022-06-05 23:58:00'")

    await tile_manager.generate_tiles_on_demand([(snowflake_tile, temp_entity_table)])

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {snowflake_tile.tile_id}"
    result = await snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5

    sql = f"SELECT * FROM {snowflake_tile.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1
    assert result["PRODUCT_ACTION"].iloc[0] == "P1"
    assert result["CUST_ID"].iloc[0] == "C1"
    assert (
        result["LAST_TILE_START_DATE"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        == last_tile_start_date_1
    )
