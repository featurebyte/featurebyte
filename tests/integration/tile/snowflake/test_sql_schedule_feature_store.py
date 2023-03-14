"""
This module contains integration tests for the stored procedure of online feature store
"""
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from pandas._testing import assert_frame_equal


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_update_feature_store__update_feature_value(session, tile_task_prep):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{agg_id}', '{date_ts_str}')"
    await session.execute_query(sql)

    sql = f"SELECT * FROM {feature_store_table_name} order by __FB_TILE_START_DATE_COLUMN"
    result = await session.execute_query(sql)
    assert len(result) == 2
    expected_df = pd.DataFrame(
        {
            "__FB_TILE_START_DATE_COLUMN": pd.to_datetime(
                ["2022-06-05 23:53:00", "2022-06-05 23:58:00"]
            ),
            "PRODUCT_ACTION": ["view", "view"],
            "CUST_ID": np.array([1, 1], dtype=np.int8),
            feature_name: np.array([3, 6], dtype=np.float64),
        }
    )
    assert_frame_equal(result, expected_df)

    number_records = 2
    update_mapping_sql = f"""
        UPDATE ONLINE_STORE_MAPPING SET SQL_QUERY = 'select {entity_col_names}, 100.0 as "{feature_name}" from TEMP_TABLE limit {number_records}'
        WHERE TILE_ID = '{tile_id}'
"""
    await session.execute_query(update_mapping_sql)

    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{agg_id}', '{date_ts_str}')"
    await session.execute_query(sql)
    sql = f"SELECT * FROM {feature_store_table_name}"
    result = await session.execute_query(sql)
    assert len(result) == 2

    expected_df = pd.DataFrame(
        {
            "__FB_TILE_START_DATE_COLUMN": pd.to_datetime(
                ["2022-06-05 23:58:00", "2022-06-05 23:53:00"]
            ),
            "PRODUCT_ACTION": ["view", "view"],
            "CUST_ID": np.array([1, 1], dtype=np.int8),
            feature_name: np.array([100, 100], dtype=np.float64),
        }
    )
    assert_frame_equal(result, expected_df)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_update_feature_store__insert_remove_feature_value(session, tile_task_prep):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{agg_id}', '{date_ts_str}')"
    await session.execute_query(sql)
    # verify existing feature store table
    sql = f"SELECT * FROM {feature_store_table_name} order by __FB_TILE_START_DATE_COLUMN"
    result = await session.execute_query(sql)
    assert len(result) == 2
    expected_df = pd.DataFrame(
        {
            "__FB_TILE_START_DATE_COLUMN": pd.to_datetime(
                ["2022-06-05 23:53:00", "2022-06-05 23:58:00"]
            ),
            "PRODUCT_ACTION": ["view", "view"],
            "CUST_ID": np.array([1, 1], dtype=np.int8),
            feature_name: np.array([3, 6], dtype=np.float64),
        }
    )
    assert_frame_equal(result, expected_df)

    # new entity universe to insert and remove records from the feature store table
    sql = f"""
        select {entity_col_names}, 99.0 as "{feature_name}" from TEMP_TABLE where __FB_TILE_START_DATE_COLUMN = ''2022-06-05 23:53:00''
        union all
        select {entity_col_names}, 98.0 as "{feature_name}" from TEMP_TABLE where __FB_TILE_START_DATE_COLUMN = ''2022-06-05 23:48:00''
"""
    update_mapping_sql = f"""
        UPDATE ONLINE_STORE_MAPPING SET SQL_QUERY = '{sql}'
        WHERE TILE_ID = '{tile_id}'
"""
    await session.execute_query(update_mapping_sql)
    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{agg_id}', '{date_ts_str}')"
    await session.execute_query(sql)

    sql = f"SELECT * FROM {feature_store_table_name} order by __FB_TILE_START_DATE_COLUMN"
    result = await session.execute_query(sql)
    assert len(result) == 3
    assert np.isnan(
        result[result.__FB_TILE_START_DATE_COLUMN == "2022-06-05 23:58:00"][feature_name].iloc[0]
    )

    expected_df = pd.DataFrame(
        {
            "__FB_TILE_START_DATE_COLUMN": pd.to_datetime(
                ["2022-06-05 23:48:00", "2022-06-05 23:53:00", "2022-06-05 23:58:00"]
            ),
            "PRODUCT_ACTION": ["view", "view", "view"],
            "CUST_ID": np.array([1, 1, 1], dtype=np.int8),
            feature_name: [np.float64(98), np.float64(99), None],
        }
    )
    assert_frame_equal(result, expected_df)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_update_feature_store__insert_with_new_feature_column(
    session, tile_task_prep
):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{agg_id}', '{date_ts_str}')"
    await session.execute_query(sql)
    # verify existing feature store table
    sql = f"SELECT * FROM {feature_store_table_name} order by __FB_TILE_START_DATE_COLUMN"
    result = await session.execute_query(sql)
    assert len(result) == 2
    expected_df = pd.DataFrame(
        {
            "__FB_TILE_START_DATE_COLUMN": pd.to_datetime(
                ["2022-06-05 23:53:00", "2022-06-05 23:58:00"]
            ),
            "PRODUCT_ACTION": ["view", "view"],
            "CUST_ID": np.array([1, 1], dtype=np.int8),
            feature_name: np.array([3, 6], dtype=np.float64),
        }
    )
    assert_frame_equal(result, expected_df)

    new_feature_name = feature_name + "_2"
    insert_new_mapping_sql = f"""
            insert into ONLINE_STORE_MAPPING(
                TILE_ID,
                AGGREGATION_ID,
                RESULT_ID,
                RESULT_TYPE,
                SQL_QUERY,
                ONLINE_STORE_TABLE_NAME,
                ENTITY_COLUMN_NAMES
            )
            values (
                '{tile_id}',
                '{agg_id}',
                '{new_feature_name}',
                'FLOAT',
                'select {entity_col_names}, cast(value_2 as float) as "{new_feature_name}" from TEMP_TABLE limit 2',
                '{feature_store_table_name}',
                '{entity_col_names}'
            )
    """
    await session.execute_query(insert_new_mapping_sql)

    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{agg_id}', '{date_ts_str}')"

    await session.execute_query(sql)

    sql = f"SELECT * FROM {feature_store_table_name} order by __FB_TILE_START_DATE_COLUMN"
    result = await session.execute_query(sql)
    assert len(result) == 2
    expected_df = pd.DataFrame(
        {
            "__FB_TILE_START_DATE_COLUMN": pd.to_datetime(
                ["2022-06-05 23:53:00", "2022-06-05 23:58:00"]
            ),
            "PRODUCT_ACTION": ["view", "view"],
            "CUST_ID": np.array([1, 1], dtype=np.int8),
            feature_name: np.array([3, 6], dtype=np.float64),
            new_feature_name: np.array([3, 6], dtype=np.float64),
        }
    )
    assert_frame_equal(result, expected_df)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_update_feature_store__insert_varchar_feature_column(
    session, tile_task_prep
):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    sql = f"""
            select {entity_col_names}, ''cat1'' as "{feature_name}" from TEMP_TABLE where __FB_TILE_START_DATE_COLUMN = ''2022-06-05 23:53:00''
    """
    update_mapping_sql = f"""
            UPDATE ONLINE_STORE_MAPPING SET SQL_QUERY = '{sql}', RESULT_TYPE = 'VARCHAR'
            WHERE TILE_ID = '{tile_id}'
    """
    await session.execute_query(update_mapping_sql)

    sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{agg_id}', '{date_ts_str}')"
    await session.execute_query(sql)
    # verify existing feature store table
    sql = f"SELECT * FROM {feature_store_table_name} order by __FB_TILE_START_DATE_COLUMN"
    result = await session.execute_query(sql)
    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "__FB_TILE_START_DATE_COLUMN": pd.to_datetime(["2022-06-05 23:53:00"]),
            "PRODUCT_ACTION": ["view"],
            "CUST_ID": np.array([1], dtype=np.int8),
            feature_name: ["cat1"],
        }
    )
    assert_frame_equal(result, expected_df)
