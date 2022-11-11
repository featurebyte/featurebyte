"""
This module contains integration tests for FeatureListManagerSnowflake
"""
import pytest

from featurebyte.enum import InternalName


@pytest.mark.asyncio
async def test_generate_tiles_on_demand(snowflake_session, snowflake_tile, feature_list_manager):
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

    await feature_list_manager.generate_tiles_on_demand([(snowflake_tile, temp_entity_table)])

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
