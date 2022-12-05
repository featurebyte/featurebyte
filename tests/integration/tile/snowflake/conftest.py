"""
This module contains common pytest config for snowflake integration tests
"""
from datetime import datetime

import pytest_asyncio


@pytest_asyncio.fixture(name="tile_task_prep")
async def tile_task_online_store_prep(snowflake_session):
    entity_col_names = "__FB_TILE_START_DATE_COLUMN,PRODUCT_ACTION,CUST_ID"
    feature_name = "feature_1"
    feature_store_table_name = "fs_table_1"

    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"

    number_records = 2
    insert_mapping_sql = f"""
            insert into TILE_FEATURE_MAPPING(
                TILE_ID, FEATURE_NAME, FEATURE_TYPE, FEATURE_VERSION, FEATURE_SQL, FEATURE_STORE_TABLE_NAME, FEATURE_ENTITY_COLUMN_NAMES
            )
            values (
                '{tile_id}', '{feature_name}', 'FLOAT', 'feature_1_v1',
                'select {entity_col_names}, cast(value_2 as float) as "{feature_name}" from {table_name} limit {number_records}',
                '{feature_store_table_name}', '{entity_col_names}'
            )
    """
    await snowflake_session.execute_query(insert_mapping_sql)

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result["FEATURE_NAME"]) == 1
    assert result["TILE_ID"].iloc[0] == tile_id
    assert result["FEATURE_NAME"].iloc[0] == feature_name

    yield tile_id, feature_store_table_name, feature_name, entity_col_names

    await snowflake_session.execute_query("DELETE FROM TILE_FEATURE_MAPPING")
    await snowflake_session.execute_query(f"DROP TABLE {feature_store_table_name}")
