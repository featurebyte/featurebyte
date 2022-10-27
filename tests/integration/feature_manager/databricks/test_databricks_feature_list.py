"""
This module contains integration tests for FeatureListManagerDatabricks
"""
import pytest

from featurebyte.enum import InternalName


@pytest.mark.asyncio
async def test_generate_tiles_on_demand(
    databricks_session, databricks_tile_spec, feature_list_manager_databricks
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

    await feature_list_manager_databricks.generate_tiles_on_demand(
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
