"""
This module contains integration tests for scheduled tile generation stored procedure
"""
from datetime import datetime
from unittest import mock

import pytest

from featurebyte.enum import InternalName
from featurebyte.sql.spark.tile_generate_schedule import TileGenerateSchedule


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_generate_tile_online(session, tile_task_prep_spark):
    """
    Test the stored procedure of generating tiles
    """

    tile_id, agg_id, feature_store_table_name, _, _ = tile_task_prep_spark

    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    tile_monitor = 10
    tile_end_ts = "2022-06-05T23:58:00Z"

    entity_col_names_str = ",".join([f"`{col}`" for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    tile_schedule_ins = TileGenerateSchedule(
        spark_session=session,
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        offline_period_minute=1440,
        tile_last_start_date_column=InternalName.TILE_LAST_START_DATE,
        tile_start_date_column=InternalName.TILE_START_DATE,
        tile_start_date_placeholder=InternalName.TILE_START_DATE_SQL_PLACEHOLDER,
        tile_end_date_placeholder=InternalName.TILE_END_DATE_SQL_PLACEHOLDER,
        monitor_periods=10,
        agg_id=agg_id,
        job_schedule_ts=tile_end_ts,
    )
    await tile_schedule_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == (tile_monitor + 1)

    # verify that feature store has been updated
    sql = f"SELECT COUNT(*) as COUNT FROM {feature_store_table_name}"
    result = await session.execute_query(sql)
    assert result["COUNT"].iloc[0] == 2

    # verify tile job monitor using sql
    sql = f"SELECT * FROM TILE_JOB_MONITOR WHERE TILE_ID = '{tile_id}' ORDER BY CREATED_AT"
    result = await session.execute_query(sql)
    assert len(result) == 4
    assert result["STATUS"].iloc[0] == "STARTED"
    assert result["STATUS"].iloc[1] == "MONITORED"
    assert result["STATUS"].iloc[2] == "GENERATED"
    assert result["STATUS"].iloc[3] == "COMPLETED"

    session_id = result["SESSION_ID"].iloc[0]
    assert "|" in session_id
    assert result["SESSION_ID"].iloc[0] == session_id
    assert result["CREATED_AT"].iloc[1] > result["CREATED_AT"].iloc[0]
    assert result["CREATED_AT"].iloc[2] > result["CREATED_AT"].iloc[1]
    assert result["CREATED_AT"].iloc[3] > result["CREATED_AT"].iloc[2]


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_monitor_tile_online(session):
    """
    Test the stored procedure of monitoring tiles
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    suffix = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{suffix}"
    agg_id = f"some_agg_id_{suffix}"
    tile_end_ts = "2022-06-05T23:53:00Z"

    table_name = f"SOURCE_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    await session.execute_query(
        f"create table {table_name} using delta as select * from TEMP_TABLE"
    )

    entity_col_names_str = ",".join([f"`{col}`" for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    tile_schedule_ins = TileGenerateSchedule(
        spark_session=session,
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        offline_period_minute=1440,
        tile_last_start_date_column=InternalName.TILE_LAST_START_DATE,
        tile_start_date_column=InternalName.TILE_START_DATE,
        tile_start_date_placeholder=InternalName.TILE_START_DATE_SQL_PLACEHOLDER,
        tile_end_date_placeholder=InternalName.TILE_END_DATE_SQL_PLACEHOLDER,
        monitor_periods=10,
        agg_id=agg_id,
        job_schedule_ts=tile_end_ts,
    )
    await tile_schedule_ins.execute()

    sql = f"""
            UPDATE {table_name} SET VALUE = VALUE + 1
            WHERE {InternalName.TILE_START_DATE} in (
                to_timestamp('2022-06-05 23:33:00', 'yyyy-MM-dd HH:mm:ss'),
                to_timestamp('2022-06-05 23:48:00', 'yyyy-MM-dd HH:mm:ss')
            )
          """
    await session.execute_query(sql)

    tile_end_ts_2 = "2022-06-05T23:58:00Z"
    tile_schedule_ins = TileGenerateSchedule(
        spark_session=session,
        featurebyte_database="TEST_DB_1",
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        offline_period_minute=1440,
        tile_last_start_date_column=InternalName.TILE_LAST_START_DATE,
        tile_start_date_column=InternalName.TILE_START_DATE,
        tile_start_date_placeholder=InternalName.TILE_START_DATE_SQL_PLACEHOLDER,
        tile_end_date_placeholder=InternalName.TILE_END_DATE_SQL_PLACEHOLDER,
        monitor_periods=10,
        agg_id=agg_id,
        job_schedule_ts=tile_end_ts_2,
    )
    await tile_schedule_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}_MONITOR"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_generate_tile__with_registry(session, tile_task_prep_spark):
    """
    Test the stored procedure of generating tiles
    """

    tile_id, agg_id, feature_store_table_name, _, _ = tile_task_prep_spark

    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    tile_monitor = 2
    tile_end_ts = "2022-06-05T23:58:00Z"

    entity_col_names_str = ",".join([f"`{col}`" for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    tile_schedule_ins = TileGenerateSchedule(
        spark_session=session,
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        offline_period_minute=1440,
        tile_last_start_date_column=InternalName.TILE_LAST_START_DATE,
        tile_start_date_column=InternalName.TILE_START_DATE,
        tile_start_date_placeholder=InternalName.TILE_START_DATE_SQL_PLACEHOLDER,
        tile_end_date_placeholder=InternalName.TILE_END_DATE_SQL_PLACEHOLDER,
        monitor_periods=tile_monitor,
        agg_id=agg_id,
        job_schedule_ts=tile_end_ts,
    )
    await tile_schedule_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == (tile_monitor + 1)

    result = await session.execute_query(
        f"SELECT LAST_TILE_START_DATE_ONLINE FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
    )
    assert (
        result["LAST_TILE_START_DATE_ONLINE"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        == "2022-06-05 23:53:00"
    )

    # test for LAST_TILE_START_DATE_ONLINE earlier than tile_start_date
    await session.execute_query(
        f"UPDATE TILE_REGISTRY SET LAST_TILE_START_DATE_ONLINE = '2022-06-05 23:33:00' WHERE TILE_ID = '{tile_id}'"
    )
    await tile_schedule_ins.execute()
    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5

    result = await session.execute_query(
        f"SELECT LAST_TILE_START_DATE_ONLINE FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
    )
    assert (
        result["LAST_TILE_START_DATE_ONLINE"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        == "2022-06-05 23:53:00"
    )
