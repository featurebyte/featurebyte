"""
This module contains integration tests for scheduled tile generation
"""
from datetime import datetime

import pytest

from featurebyte.enum import InternalName
from featurebyte.sql.common import construct_create_table_query
from featurebyte.sql.tile_generate_schedule import TileGenerateSchedule


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_generate_tile_online(session, tile_task_prep_spark, base_sql_model):
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

    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f" SELECT INDEX,{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    tile_schedule_ins = TileGenerateSchedule(
        session=session,
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
        aggregation_id=agg_id,
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


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_monitor_tile_online(session, base_sql_model):
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
    create_sql = construct_create_table_query(
        table_name, "select * from TEMP_TABLE", session=session
    )
    await session.execute_query(create_sql)

    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f" SELECT INDEX,{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    tile_schedule_ins = TileGenerateSchedule(
        session=session,
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
        aggregation_id=agg_id,
        job_schedule_ts=tile_end_ts,
    )
    await tile_schedule_ins.execute()

    sql = f"""
            UPDATE {table_name} SET VALUE = VALUE + 1
            WHERE {InternalName.TILE_START_DATE} in (
                to_timestamp('2022-06-05 23:33:00'),
                to_timestamp('2022-06-05 23:48:00')
            )
          """
    await session.execute_query(sql)

    tile_end_ts_2 = "2022-06-05T23:58:03Z"
    tile_schedule_ins = TileGenerateSchedule(
        session=session,
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
        aggregation_id=agg_id,
        job_schedule_ts=tile_end_ts_2,
    )
    await tile_schedule_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}_MONITOR"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_generate_tile__with_registry(session, tile_task_prep_spark, base_sql_model):
    """
    Test the stored procedure of generating tiles
    """

    tile_id, agg_id, feature_store_table_name, _, _ = tile_task_prep_spark

    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    tile_monitor = 2
    tile_end_ts = "2022-06-05T23:58:03Z"

    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f" SELECT INDEX,{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    tile_schedule_ins = TileGenerateSchedule(
        session=session,
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
        aggregation_id=agg_id,
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
        == "2022-06-05 23:58:00"
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
        == "2022-06-05 23:58:00"
    )


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_generate_tile__no_default_job_ts(
    session, tile_task_prep_spark, base_sql_model
):
    """
    Test the stored procedure of generating tiles
    """

    tile_id, agg_id, feature_store_table_name, _, _ = tile_task_prep_spark

    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    tile_monitor = 2

    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f" SELECT INDEX,{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    date_format = "%Y-%m-%d %H:%M:%S"
    tile_modulo_frequency_second = 3
    blind_spot_second = 3
    frequency_minute = 1

    # job scheduled time falls on exactly the same time as next job time
    used_job_schedule_ts = "2023-05-04 14:33:03"
    tile_schedule_ins = TileGenerateSchedule(
        session=session,
        tile_id=tile_id,
        tile_modulo_frequency_second=tile_modulo_frequency_second,
        blind_spot_second=blind_spot_second,
        frequency_minute=frequency_minute,
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
        aggregation_id=agg_id,
        job_schedule_ts=used_job_schedule_ts,
    )
    await tile_schedule_ins.execute()
    result = await session.execute_query(
        f"SELECT LAST_TILE_START_DATE_ONLINE FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
    )
    assert (
        result["LAST_TILE_START_DATE_ONLINE"].iloc[0].strftime(date_format) == "2023-05-04 14:33:00"
    )

    # job scheduled time falls on in-between job times
    used_job_schedule_ts = "2023-05-04 14:33:30"
    tile_schedule_ins = TileGenerateSchedule(
        session=session,
        tile_id=tile_id,
        tile_modulo_frequency_second=tile_modulo_frequency_second,
        blind_spot_second=blind_spot_second,
        frequency_minute=frequency_minute,
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
        aggregation_id=agg_id,
        job_schedule_ts=used_job_schedule_ts,
    )
    await tile_schedule_ins.execute()
    result = await session.execute_query(
        f"SELECT LAST_TILE_START_DATE_ONLINE FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
    )
    assert (
        result["LAST_TILE_START_DATE_ONLINE"].iloc[0].strftime(date_format) == "2023-05-04 14:33:00"
    )
