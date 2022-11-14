import pytest

from featurebyte.enum import InternalName
from featurebyte.models.tile import TileType

pytest.skip(allow_module_level=True)


@pytest.mark.asyncio
async def test_databricks_generate_tile(
    databricks_tile_spec, databricks_session, tile_manager_databricks
):
    """
    Test generate_tiles method in TileManagerDatabricks
    """

    await tile_manager_databricks.generate_tiles(
        databricks_tile_spec,
        TileType.ONLINE,
        "2022-10-13 23:33:00",
        "2022-10-13 23:58:00",
        "2022-10-13 23:53:00",
    )

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {databricks_tile_spec.tile_id}"
    result = await databricks_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5


@pytest.mark.asyncio
async def test_update_tile_entity_tracker(
    databricks_tile_spec, databricks_session, tile_manager_databricks
):
    """
    Test update_tile_entity_tracker method in TileSnowflake
    """

    temp_entity_table = "TEMP_ENTITY_TRACKER_2"
    last_tile_start_date_1 = "2022-10-26 10:52:14"
    last_tile_start_date_2 = "2022-10-27 10:52:14"

    await databricks_session.execute_query(
        f"CREATE TABLE {temp_entity_table} (PRODUCT_ACTION VARCHAR(255), CUST_ID VARCHAR(255), LAST_TILE_START_DATE TIMESTAMP)"
    )
    await databricks_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P1', 'C1', '{last_tile_start_date_1}') "
    )
    await databricks_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P2', 'C2', '{last_tile_start_date_2}') "
    )

    await tile_manager_databricks.update_tile_entity_tracker(
        tile_spec=databricks_tile_spec, temp_entity_table=temp_entity_table
    )

    sql = f"SELECT * FROM {databricks_tile_spec.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await databricks_session.execute_query(sql)
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

    last_tile_start_date_2_new = "2022-10-28 00:00:00"
    await databricks_session.execute_query(
        f"UPDATE {temp_entity_table} SET LAST_TILE_START_DATE = '{last_tile_start_date_2_new}' WHERE PRODUCT_ACTION = 'P2'"
    )
    last_tile_start_date_3 = "2022-10-28 10:52:14"
    await databricks_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P3', 'C3', '{last_tile_start_date_3}') "
    )

    await tile_manager_databricks.update_tile_entity_tracker(
        tile_spec=databricks_tile_spec, temp_entity_table=temp_entity_table
    )

    sql = f"SELECT * FROM {databricks_tile_spec.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await databricks_session.execute_query(sql)
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
async def test_generate_tiles_on_demand(
    databricks_session, databricks_tile_spec, tile_manager_databricks
):
    """
    Test generate_tiles_on_demand
    """
    temp_entity_table = "TEMP_ENTITY_TRACKER_1"
    last_tile_start_date_1 = "2022-10-27 10:52:14"
    await databricks_session.execute_query(
        f"CREATE TABLE {temp_entity_table} (PRODUCT_ACTION VARCHAR(255), CUST_ID VARCHAR(255), LAST_TILE_START_DATE TIMESTAMP)"
    )
    await databricks_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P1', 'C1', '{last_tile_start_date_1}') "
    )

    databricks_tile_spec.tile_sql = databricks_tile_spec.tile_sql.replace(
        InternalName.TILE_START_DATE_SQL_PLACEHOLDER, "'2022-10-13 23:33:00'"
    ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, "'2022-10-13 23:58:00'")

    await tile_manager_databricks.generate_tiles_on_demand(
        [(databricks_tile_spec, temp_entity_table)]
    )

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {databricks_tile_spec.tile_id}"
    result = await databricks_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5

    sql = f"SELECT * FROM {databricks_tile_spec.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await databricks_session.execute_query(sql)
    assert len(result) == 1
    assert result["PRODUCT_ACTION"].iloc[0] == "P1"
    assert result["CUST_ID"].iloc[0] == "C1"
    assert (
        result["LAST_TILE_START_DATE"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        == last_tile_start_date_1
    )
