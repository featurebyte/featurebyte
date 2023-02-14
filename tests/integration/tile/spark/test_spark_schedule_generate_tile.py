"""
This module contains integration tests for scheduled tile generation stored procedure
"""
from datetime import datetime

from featurebyte.enum import InternalName
from featurebyte.sql.spark.tile_generate_schedule import TileGenerateSchedule


def test_schedule_generate_tile_online(spark_session, tile_task_prep_spark):
    """
    Test the stored procedure of generating tiles
    """

    tile_id, agg_id, feature_store_table_name, _, _ = tile_task_prep_spark

    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "`客户`"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    tile_monitor = 10
    tile_end_ts = "2022-06-05T23:58:00Z"

    entity_col_names_str = ",".join(entity_col_names)
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    tile_schedule_ins = TileGenerateSchedule(
        spark_session=spark_session,
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
        job_schedule_ts=tile_end_ts,
    )
    tile_schedule_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = spark_session.sql(sql).collect()
    assert result[0]["TILE_COUNT"] == (tile_monitor + 1)

    # verify that feature store has been updated
    sql = f"SELECT COUNT(*) as COUNT FROM {feature_store_table_name}"
    result = spark_session.sql(sql).collect()
    assert result[0]["COUNT"] == 2

    # verify tile job monitor using sql
    sql = f"SELECT * FROM TILE_JOB_MONITOR WHERE TILE_ID = '{tile_id}'"
    result = spark_session.sql(sql).orderBy("CREATED_AT").collect()
    assert len(result) == 4
    assert result[0]["STATUS"] == "STARTED"
    assert result[1]["STATUS"] == "MONITORED"
    assert result[2]["STATUS"] == "GENERATED"
    assert result[3]["STATUS"] == "COMPLETED"

    session_id = result[0]["SESSION_ID"]
    assert "|" in session_id
    assert result[0]["SESSION_ID"] == session_id
    assert result[1]["CREATED_AT"] > result[0]["CREATED_AT"]
    assert result[2]["CREATED_AT"] > result[1]["CREATED_AT"]
    assert result[3]["CREATED_AT"] > result[2]["CREATED_AT"]


def test_schedule_monitor_tile_online(spark_session):
    """
    Test the stored procedure of monitoring tiles
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "`客户`"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    suffix = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{suffix}"
    agg_id = f"some_agg_id_{suffix}"
    tile_end_ts = "2022-06-05T23:53:00Z"

    entity_col_names_str = ",".join(entity_col_names)
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f" SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    tile_schedule_ins = TileGenerateSchedule(
        spark_session=spark_session,
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
        job_schedule_ts=tile_end_ts,
    )
    tile_schedule_ins.execute()

    sql = f"""
            UPDATE {table_name} SET VALUE = VALUE + 1
            WHERE {InternalName.TILE_START_DATE} in (
                to_timestamp('2022-06-05 23:33:00', 'yyyy-MM-dd HH:mm:ss'),
                to_timestamp('2022-06-05 23:48:00', 'yyyy-MM-dd HH:mm:ss')
            )
          """
    spark_session.sql(sql)

    tile_end_ts_2 = "2022-06-05T23:58:00Z"
    tile_schedule_ins = TileGenerateSchedule(
        spark_session=spark_session,
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
    tile_schedule_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}_MONITOR"
    result = spark_session.sql(sql).collect()
    assert result[0]["TILE_COUNT"] == 2

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = spark_session.sql(sql).collect()
    assert result[0]["TILE_COUNT"] == 2
