"""
This module contains integration tests for the stored procedure of online feature store
"""
import pytest


@pytest.mark.asyncio
async def test_schedule_update_feature_store(snowflake_session, tile_task_prep):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep
    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{tile_id}')"
    await snowflake_session.execute_query(sql)

    sql = f"SELECT COUNT(*) as COUNT FROM {feature_store_table_name}"
    result = await snowflake_session.execute_query(sql)
    assert result["COUNT"].iloc[0] == 5

    number_records = 10
    update_mapping_sql = f"""
        UPDATE TILE_FEATURE_MAPPING SET FEATURE_SQL = 'select {entity_col_names}, value as {feature_name} from TEMP_TABLE limit {number_records}'
        WHERE TILE_ID = '{tile_id}'
"""
    await snowflake_session.execute_query(update_mapping_sql)

    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{tile_id}')"
    await snowflake_session.execute_query(sql)

    sql = f"SELECT COUNT(*) as COUNT FROM {feature_store_table_name}"
    result = await snowflake_session.execute_query(sql)
    assert result["COUNT"].iloc[0] == 10
