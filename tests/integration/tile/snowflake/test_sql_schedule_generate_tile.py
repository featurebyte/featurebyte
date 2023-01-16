"""
This module contains integration tests for scheduled tile generation stored procedure
"""
from datetime import datetime

import pytest
from pandas.testing import assert_frame_equal
from snowflake.connector import ProgrammingError

from featurebyte.enum import InternalName


@pytest.mark.asyncio
async def test_schedule_generate_tile_online(snowflake_session, tile_task_prep, tile_manager):
    """
    Test the stored procedure of generating tiles
    """

    tile_id, feature_store_table_name, _, _ = tile_task_prep

    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_monitor = 10
    tile_end_ts = "2022-06-05T23:58:00Z"
    tile_sql = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    sql = f"""
        call SP_TILE_GENERATE_SCHEDULE(
          '{tile_id}',
          183,
          3,
          5,
          1440,
          '{tile_sql}',
          '{InternalName.TILE_START_DATE}',
          '{InternalName.TILE_LAST_START_DATE}',
          '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}',
          '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}',
          '{entity_col_names}',
          '{value_col_names}',
          '{value_col_types}',
          'ONLINE',
          {tile_monitor},
          '{tile_end_ts}'
        )
        """
    result = await snowflake_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE_SCHEDULE"].iloc[0]

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = await snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == (tile_monitor + 1)

    # verify that feature store has been updated
    sql = f"SELECT COUNT(*) as COUNT FROM {feature_store_table_name}"
    result = await snowflake_session.execute_query(sql)
    assert result["COUNT"].iloc[0] == 2

    # verify tile job monitor using sql
    sql = f"SELECT * FROM TILE_JOB_MONITOR WHERE TILE_ID = '{tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 4
    assert result["STATUS"].tolist() == ["STARTED", "MONITORED", "GENERATED", "COMPLETED"]
    assert result["TILE_TYPE"].tolist() == ["ONLINE", "ONLINE", "ONLINE", "ONLINE"]
    assert result["MESSAGE"].tolist() == ["", "", "", ""]
    session_id = result["SESSION_ID"].iloc[0]
    assert "|" in session_id
    assert result["SESSION_ID"].tolist() == [session_id, session_id, session_id, session_id]
    assert result["CREATED_AT"].iloc[1] > result["CREATED_AT"].iloc[0]
    assert result["CREATED_AT"].iloc[2] > result["CREATED_AT"].iloc[1]
    assert result["CREATED_AT"].iloc[3] > result["CREATED_AT"].iloc[2]

    # verify that all tile job monitor records retrieved using tile_manager are the same
    result2 = await tile_manager.retrieve_tile_job_audit_logs(start_date=datetime(1970, 1, 1))
    assert_frame_equal(result, result2)

    # verify tile job monitor records using tile_manager with start_date and end_date
    end_date = result["CREATED_AT"].iloc[1]
    result2 = await tile_manager.retrieve_tile_job_audit_logs(
        start_date=datetime(1970, 1, 1), end_date=end_date
    )
    assert len(result2) == 2

    # verify tile job monitor records using tile_manager with start_date, end_date and tile_id
    end_date = result["CREATED_AT"].iloc[1]
    result2 = await tile_manager.retrieve_tile_job_audit_logs(
        start_date=datetime(1970, 1, 1), end_date=end_date, tile_id=tile_id
    )
    assert len(result2) == 2


@pytest.mark.asyncio
async def test_tile_job_monitor__fail_halfway(snowflake_session, tile_task_prep):
    """
    Test the stored procedure of generating tiles
    """

    tile_id, feature_store_table_name, _, _ = tile_task_prep

    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_monitor = 10
    tile_end_ts = "2022-06-05T23:58:00Z"
    tile_sql = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    # simulate error for the stored procedure to stop half way
    await snowflake_session.execute_query(
        "ALTER TABLE TILE_REGISTRY RENAME COLUMN TILE_ID to TILE_ID_TEMP"
    )
    sql = f"""
        call SP_TILE_GENERATE_SCHEDULE(
          '{tile_id}',
          183,
          3,
          5,
          1440,
          '{tile_sql}',
          '{InternalName.TILE_START_DATE}',
          '{InternalName.TILE_LAST_START_DATE}',
          '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}',
          '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}',
          '{entity_col_names}',
          '{value_col_names}',
          '{value_col_types}',
          'ONLINE',
          {tile_monitor},
          '{tile_end_ts}'
        )
        """
    try:
        await snowflake_session.execute_query(sql)
    except:
        await snowflake_session.execute_query(
            "ALTER TABLE TILE_REGISTRY RENAME COLUMN TILE_ID_TEMP to TILE_ID"
        )

    # verify tile job monitor
    sql = f"SELECT * FROM TILE_JOB_MONITOR WHERE TILE_ID = '{tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 3
    assert result["STATUS"].tolist() == ["STARTED", "MONITORED", "GENERATED_FAILED"]
    error_msg = result["MESSAGE"].iloc[2]
    assert "error" in error_msg and "TILE_ID" in error_msg

    session_id = result["SESSION_ID"].iloc[0]
    assert result["SESSION_ID"].tolist() == [session_id] * 3
    assert result["CREATED_AT"].iloc[1] > result["CREATED_AT"].iloc[0]
    assert result["CREATED_AT"].iloc[2] > result["CREATED_AT"].iloc[1]


@pytest.mark.asyncio
async def test_schedule_monitor_tile_online(snowflake_session):
    """
    Test the stored procedure of monitoring tiles
    """
    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_monitor = 10
    tile_end_ts = "2022-06-05T23:53:00Z"
    tile_sql = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    sql = f"""
        call SP_TILE_GENERATE_SCHEDULE(
          '{tile_id}',
          183,
          3,
          5,
          1440,
          '{tile_sql}',
          '{InternalName.TILE_START_DATE}',
          '{InternalName.TILE_LAST_START_DATE}',
          '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}',
          '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}',
          '{entity_col_names}',
          '{value_col_names}',
          '{value_col_types}',
          'ONLINE',
          {tile_monitor},
          '{tile_end_ts}'
        )
        """
    result = await snowflake_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE_SCHEDULE"].iloc[0]

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1 WHERE {InternalName.TILE_START_DATE} in ('2022-06-05T23:48:00Z', '2022-06-05T23:33:00Z') "
    await snowflake_session.execute_query(sql)

    tile_end_ts_2 = "2022-06-05T23:58:00Z"
    sql = f"""
        call SP_TILE_GENERATE_SCHEDULE(
          '{tile_id}',
          183,
          3,
          5,
          1440,
          '{tile_sql}',
          '{InternalName.TILE_START_DATE}',
          '{InternalName.TILE_LAST_START_DATE}',
          '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}',
          '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}',
          '{entity_col_names}',
          '{value_col_names}',
          '{value_col_types}',
          'ONLINE',
          {tile_monitor},
          '{tile_end_ts_2}'
        )
        """
    await snowflake_session.execute_query(sql)

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}_MONITOR"
    result = await snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2


@pytest.mark.asyncio
async def test_schedule_monitor_tile_existing_new_column(snowflake_session):
    """
    Test the stored procedure of monitoring tiles
    """
    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_monitor = 10
    tile_end_ts = "2022-06-05T23:53:00Z"
    tile_sql = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    sql = f"""
        call SP_TILE_GENERATE_SCHEDULE(
          '{tile_id}',
          183,
          3,
          5,
          1440,
          '{tile_sql}',
          '{InternalName.TILE_START_DATE}',
          '{InternalName.TILE_LAST_START_DATE}',
          '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}',
          '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}',
          '{entity_col_names}',
          '{value_col_names}',
          '{value_col_types}',
          'ONLINE',
          {tile_monitor},
          '{tile_end_ts}'
        )
        """
    result = await snowflake_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE_SCHEDULE"].iloc[0]

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1 WHERE {InternalName.TILE_START_DATE} in ('2022-06-05T23:48:00Z', '2022-06-05T23:33:00Z') "
    await snowflake_session.execute_query(sql)

    value_col_names_2 = "VALUE,VALUE_2"
    value_col_types_2 = "FLOAT,FLOAT"
    tile_sql_2 = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names_2} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )
    tile_end_ts_2 = "2022-06-05T23:58:00Z"
    sql = f"""
        call SP_TILE_GENERATE_SCHEDULE(
          '{tile_id}',
          183,
          3,
          5,
          1440,
          '{tile_sql_2}',
          '{InternalName.TILE_START_DATE}',
          '{InternalName.TILE_LAST_START_DATE}',
          '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}',
          '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}',
          '{entity_col_names}',
          '{value_col_names_2}',
          '{value_col_types_2}',
          'ONLINE',
          {tile_monitor},
          '{tile_end_ts_2}'
        )
        """
    await snowflake_session.execute_query(sql)

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}_MONITOR"
    result = await snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2


@pytest.mark.asyncio
async def test_schedule_monitor_tile_all_new_column(snowflake_session):
    """
    Test the stored procedure of monitoring tiles
    """
    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_monitor = 10
    tile_end_ts = "2022-06-05T23:53:00Z"
    tile_sql = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    sql = f"""
        call SP_TILE_GENERATE_SCHEDULE(
          '{tile_id}',
          183,
          3,
          5,
          1440,
          '{tile_sql}',
          '{InternalName.TILE_START_DATE}',
          '{InternalName.TILE_LAST_START_DATE}',
          '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}',
          '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}',
          '{entity_col_names}',
          '{value_col_names}',
          '{value_col_types}',
          'ONLINE',
          {tile_monitor},
          '{tile_end_ts}'
        )
        """
    result = await snowflake_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE_SCHEDULE"].iloc[0]

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1 WHERE {InternalName.TILE_START_DATE} in ('2022-06-05T23:48:00Z', '2022-06-05T23:33:00Z') "
    await snowflake_session.execute_query(sql)

    value_col_names_2 = "VALUE_3"
    value_col_types_2 = "FLOAT"
    tile_sql_2 = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names_2} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )
    tile_end_ts_2 = "2022-06-05T23:58:00Z"
    monitor_sql = f"""
        call SP_TILE_GENERATE_SCHEDULE(
          '{tile_id}',
          183,
          3,
          5,
          1440,
          '{tile_sql_2}',
          '{InternalName.TILE_START_DATE}',
          '{InternalName.TILE_LAST_START_DATE}',
          '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}',
          '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}',
          '{entity_col_names}',
          '{value_col_names_2}',
          '{value_col_types_2}',
          'ONLINE',
          {tile_monitor},
          '{tile_end_ts_2}'
        )
        """
    await snowflake_session.execute_query(monitor_sql)

    with pytest.raises(ProgrammingError) as excinfo:
        sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}_MONITOR"
        await snowflake_session.execute_query(sql)
    assert f"Object '{tile_id}_MONITOR' does not exist or not authorized" in str(excinfo.value)

    update_sql = f"UPDATE {table_name} SET VALUE_3 = VALUE_3 + 1 WHERE {InternalName.TILE_START_DATE} in ('2022-06-05T23:48:00Z', '2022-06-05T23:33:00Z') "
    await snowflake_session.execute_query(update_sql)
    await snowflake_session.execute_query(monitor_sql)
    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}_MONITOR"
    result = await snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2
