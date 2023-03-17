"""
This module contains integration tests for the stored procedure of online feature store
"""
from datetime import datetime

import numpy as np
import pytest

from featurebyte.sql.spark.tile_schedule_online_store import TileScheduleOnlineStore


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_update_feature_store__update_feature_value(session, tile_task_prep_spark):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep_spark
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    tile_online_store_ins = TileScheduleOnlineStore(
        spark_session=session,
        agg_id=agg_id,
        job_schedule_ts_str=date_ts_str,
    )
    await tile_online_store_ins.execute()

    sql = f"SELECT * FROM {feature_store_table_name} order by __FB_TILE_START_DATE_COLUMN"
    result = await session.execute_query(sql)
    assert len(result) == 2
    assert result[feature_name].iloc[0] == 3
    assert result["PRODUCT_ACTION"].iloc[0] == "view"
    assert result[feature_name].iloc[1] == 6
    assert result["PRODUCT_ACTION"].iloc[1] == "view"

    number_records = 2
    update_mapping_sql = f"""
        UPDATE ONLINE_STORE_MAPPING SET SQL_QUERY = 'select {entity_col_names}, 100.0 as {feature_name} from TEMP_TABLE limit {number_records}'
        WHERE TILE_ID = '{tile_id}'
"""
    await session.execute_query(update_mapping_sql)

    # sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{agg_id}', '{date_ts_str}')"
    # spark_session.sql(sql)
    tile_online_store_ins = TileScheduleOnlineStore(
        spark_session=session,
        agg_id=agg_id,
        job_schedule_ts_str=date_ts_str,
    )
    await tile_online_store_ins.execute()

    sql = f"SELECT * FROM {feature_store_table_name}"
    result = await session.execute_query(sql)
    assert len(result) == 2
    assert result[feature_name].iloc[0] == 100
    assert result["PRODUCT_ACTION"].iloc[0] == "view"
    assert result[feature_name].iloc[1] == 100
    assert result["PRODUCT_ACTION"].iloc[1] == "view"


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_update_feature_store__insert_with_new_feature_column(
    session, tile_task_prep_spark
):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep_spark
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    tile_online_store_ins = TileScheduleOnlineStore(
        spark_session=session,
        agg_id=agg_id,
        job_schedule_ts_str=date_ts_str,
    )
    await tile_online_store_ins.execute()

    # verify existing feature store table
    sql = f"SELECT * FROM {feature_store_table_name} order by __FB_TILE_START_DATE_COLUMN"
    result = await session.execute_query(sql)
    assert len(result) == 2
    assert result[feature_name].iloc[0] == 3
    assert result["PRODUCT_ACTION"].iloc[0] == "view"
    assert result[feature_name].iloc[1] == 6
    assert result["PRODUCT_ACTION"].iloc[1] == "view"

    new_feature_name = feature_name + "_2"
    insert_new_mapping_sql = f"""
            insert into ONLINE_STORE_MAPPING(
                TILE_ID,
                AGGREGATION_ID,
                RESULT_ID,
                RESULT_TYPE,
                SQL_QUERY,
                ONLINE_STORE_TABLE_NAME,
                ENTITY_COLUMN_NAMES,
                IS_DELETED,
                CREATED_AT
            )
            values (
                '{tile_id}',
                '{agg_id}',
                '{new_feature_name}',
                'FLOAT',
                'select {entity_col_names}, cast(value_2 as float) as {new_feature_name} from TEMP_TABLE limit 2',
                '{feature_store_table_name}',
                '{entity_col_names}',
                false,
                current_timestamp()
            )
    """
    await session.execute_query(insert_new_mapping_sql)

    tile_online_store_ins = TileScheduleOnlineStore(
        spark_session=session,
        agg_id=agg_id,
        job_schedule_ts_str=date_ts_str,
    )
    await tile_online_store_ins.execute()

    sql = f"SELECT * FROM {feature_store_table_name} order by __FB_TILE_START_DATE_COLUMN"
    result = await session.execute_query(sql)
    assert len(result) == 2
    assert result[feature_name].iloc[0] == 3
    assert result[new_feature_name].iloc[0] == 3
    assert result[feature_name].iloc[1] == 6
    assert result[new_feature_name].iloc[1] == 6


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_update_feature_store__insert_varchar_feature_column(
    session, tile_task_prep_spark
):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep_spark
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    sql = f"""
            select {entity_col_names}, \\'cat1\\' as {feature_name} from TEMP_TABLE where __FB_TILE_START_DATE_COLUMN = \\'2022-06-05 23:53:00\\'
    """
    update_mapping_sql = f"""
            UPDATE ONLINE_STORE_MAPPING SET SQL_QUERY = '{sql}', RESULT_TYPE = 'VARCHAR'
            WHERE TILE_ID = '{tile_id}'
    """
    await session.execute_query(update_mapping_sql)

    tile_online_store_ins = TileScheduleOnlineStore(
        spark_session=session,
        agg_id=agg_id,
        job_schedule_ts_str=date_ts_str,
    )
    await tile_online_store_ins.execute()

    # verify existing feature store table
    sql = f"SELECT * FROM {feature_store_table_name} order by __FB_TILE_START_DATE_COLUMN"
    result = await session.execute_query(sql)
    assert len(result) == 1
    assert result[feature_name].iloc[0] == "cat1"


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_online_feature_store__change_entity_universe(session, tile_task_prep_spark):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep_spark
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    tile_online_store_ins = TileScheduleOnlineStore(
        spark_session=session,
        agg_id=agg_id,
        job_schedule_ts_str=date_ts_str,
    )
    await tile_online_store_ins.execute()

    sql = f"SELECT * FROM {feature_store_table_name} order by __FB_TILE_START_DATE_COLUMN"
    result = await session.execute_query(sql)
    assert len(result) == 2
    assert result[feature_name].iloc[0] == 3
    assert result["PRODUCT_ACTION"].iloc[0] == "view"
    assert result[feature_name].iloc[1] == 6
    assert result["PRODUCT_ACTION"].iloc[1] == "view"

    new_select_sql = f"select {entity_col_names}, 100.0 as {feature_name} from TEMP_TABLE ORDER BY __FB_TILE_START_DATE_COLUMN ASC limit 2"
    update_mapping_sql = f"""
        UPDATE ONLINE_STORE_MAPPING
        SET SQL_QUERY = '{new_select_sql}'
        WHERE TILE_ID = '{tile_id}'
"""
    await session.execute_query(update_mapping_sql)

    # sql = f"call SP_TILE_SCHEDULE_ONLINE_STORE('{agg_id}', '{date_ts_str}')"
    # spark_session.sql(sql)
    tile_online_store_ins = TileScheduleOnlineStore(
        spark_session=session,
        agg_id=agg_id,
        job_schedule_ts_str=date_ts_str,
    )
    await tile_online_store_ins.execute()

    sql = f"SELECT * FROM {feature_store_table_name} ORDER BY UPDATED_AT"
    result = await session.execute_query(sql)
    assert len(result) == 4
    assert np.isnan(result[feature_name].iloc[0])
    assert result["PRODUCT_ACTION"].iloc[0] == "view"
    assert np.isnan(result[feature_name].iloc[1])
    assert result["PRODUCT_ACTION"].iloc[1] == "view"
    assert result[feature_name].iloc[2] == 100
    assert result["PRODUCT_ACTION"].iloc[2] == "action"
    assert result[feature_name].iloc[3] == 100
    assert result["PRODUCT_ACTION"].iloc[3] == "action"
