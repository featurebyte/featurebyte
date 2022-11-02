"""
This module contains integration tests for the stored procedure of online feature store
"""
from datetime import datetime

import pytest


@pytest.mark.asyncio
async def test_schedule_update_feature_store(snowflake_session):
    """
    Test the stored procedure for updating feature store
    """
    entity_col_names = "__FB_TILE_START_DATE_COLUMN,PRODUCT_ACTION,CUST_ID"
    feature_name = "feature_1"
    feature_store_table_name = "fs_table_1"

    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"

    number_records = 5
    insert_mapping_sql = f"""
        insert into TILE_FEATURE_MAPPING(
            TILE_ID, FEATURE_NAME, FEATURE_VERSION, FEATURE_SQL, FEATURE_STORE_TABLE_NAME, FEATURE_ENTITY_COLUMN_NAMES
        )
        values (
            '{tile_id}', '{feature_name}', 'feature_1_v1',
            'select {entity_col_names}, value as {feature_name} from {table_name} limit {number_records}', '{feature_store_table_name}',
            '{entity_col_names}'
        )
"""
    await snowflake_session.execute_query(insert_mapping_sql)

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result["FEATURE_NAME"]) == 1
    assert result["TILE_ID"].iloc[0] == tile_id
    assert result["FEATURE_NAME"].iloc[0] == feature_name

    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{tile_id}')"
    await snowflake_session.execute_query(sql)

    sql = f"SELECT COUNT(*) as COUNT FROM {feature_store_table_name}"
    result = await snowflake_session.execute_query(sql)
    assert result["COUNT"].iloc[0] == 5

    number_records = 10
    update_mapping_sql = f"""
        UPDATE TILE_FEATURE_MAPPING SET FEATURE_SQL = 'select {entity_col_names}, value as {feature_name} from {table_name} limit {number_records}'
        WHERE TILE_ID = '{tile_id}'
"""
    await snowflake_session.execute_query(update_mapping_sql)

    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{tile_id}')"
    await snowflake_session.execute_query(sql)

    sql = f"SELECT COUNT(*) as COUNT FROM {feature_store_table_name}"
    result = await snowflake_session.execute_query(sql)
    assert result["COUNT"].iloc[0] == 10
