"""
Tests for featurebyte/query_graph/sql/cron.py
"""

from featurebyte import CronFeatureJobSetting, Crontab
from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.cron import (
    JobScheduleTable,
    JobScheduleTableSet,
    get_request_table_job_datetime_column_name,
    get_request_table_join_all_job_schedules_expr,
    get_unique_cron_feature_job_settings,
)
from tests.util.helper import get_sql_adapter_from_source_type


def test_get_cron_feature_job_settings(global_graph, time_series_window_aggregate_feature_node):
    """
    Test get_unique_cron_feature_job_settings
    """
    cron_feature_job_settings = get_unique_cron_feature_job_settings(
        global_graph,
        [time_series_window_aggregate_feature_node, time_series_window_aggregate_feature_node],
        SourceType.SNOWFLAKE,
    )
    assert cron_feature_job_settings == [
        CronFeatureJobSetting(
            crontab=Crontab(minute=0, hour=0, day_of_month="*", month_of_year="*", day_of_week="*"),
            timezone="Etc/UTC",
            reference_timezone="Asia/Singapore",
        )
    ]


def test_get_request_table_with_job_schedule_name():
    """
    Test get_request_table_with_job_schedule_name
    """
    table_name = get_request_table_job_datetime_column_name(
        CronFeatureJobSetting(crontab="0 0 * * *", timezone="Asia/Singapore"),
        SourceType.SNOWFLAKE,
    )
    assert table_name == "__FB_CRON_JOB_SCHEDULE_DATETIME_0 0 * * *_Asia/Singapore_None"


def test_get_request_table_with_job_schedule_name_with_reference_tz():
    """
    Test get_request_table_with_job_schedule_name
    """
    table_name = get_request_table_job_datetime_column_name(
        CronFeatureJobSetting(
            crontab="0 0 * * *", timezone="Asia/Singapore", reference_timezone="Asia/Tokyo"
        ),
        SourceType.SNOWFLAKE,
    )
    assert table_name == "__FB_CRON_JOB_SCHEDULE_DATETIME_0 0 * * *_Asia/Singapore_Asia/Tokyo"


def test_get_request_table_with_job_schedule_name_bigquery():
    """
    Test get_request_table_with_job_schedule_name for BigQuery
    """
    table_name = get_request_table_job_datetime_column_name(
        CronFeatureJobSetting(crontab="0 0 * * *", timezone="Asia/Singapore"),
        SourceType.BIGQUERY,
    )
    # Should use hash for BigQuery since original name contains invalid characters (*)
    assert table_name.startswith("__FB_CRON_JOB_SCHEDULE_DATETIME_")
    assert len(table_name) == len("__FB_CRON_JOB_SCHEDULE_DATETIME_") + 8  # 8-char hash
    assert "*" not in table_name  # Should not contain invalid characters


def test_get_cron_feature_job_settings__blind_spot_handling(
    global_graph,
    time_series_window_aggregate_feature_node,
    time_series_window_aggregate_with_blind_spot_feature_node,
):
    """
    Test get_unique_cron_feature_job_settings
    """
    cron_feature_job_settings = get_unique_cron_feature_job_settings(
        global_graph,
        [
            time_series_window_aggregate_feature_node,
            time_series_window_aggregate_with_blind_spot_feature_node,
        ],
        SourceType.SNOWFLAKE,
    )
    assert cron_feature_job_settings == [
        CronFeatureJobSetting(
            crontab=Crontab(minute=0, hour=0, day_of_month="*", month_of_year="*", day_of_week="*"),
            timezone="Etc/UTC",
            reference_timezone="Asia/Singapore",
        )
    ]


def test_get_request_table_join_all_job_schedules_expr_multiple_tables():
    """
    Test get_request_table_join_all_job_schedules_expr with multiple job schedule tables
    to ensure that column names are properly tracked across iterations.
    """
    # Setup
    adapter = get_sql_adapter_from_source_type(SourceType.SNOWFLAKE)
    request_table_name = "REQUEST_TABLE"
    request_table_columns = ["col1", "col2", "point_in_time"]

    # Create two different job schedule tables with different cron settings
    job_schedule_table_1 = JobScheduleTable(
        table_name="job_schedule_1",
        cron_feature_job_setting=CronFeatureJobSetting(crontab="0 0 * * *", timezone="UTC"),
    )
    job_schedule_table_2 = JobScheduleTable(
        table_name="job_schedule_2",
        cron_feature_job_setting=CronFeatureJobSetting(crontab="0 12 * * *", timezone="UTC"),
    )

    job_schedule_table_set = JobScheduleTableSet(
        tables=[job_schedule_table_1, job_schedule_table_2]
    )

    # Execute
    result_expr, job_datetime_columns = get_request_table_join_all_job_schedules_expr(
        request_table_name=request_table_name,
        request_table_columns=request_table_columns,
        job_schedule_table_set=job_schedule_table_set,
        adapter=adapter,
    )

    # Verify
    assert len(job_datetime_columns) == 2
    assert job_datetime_columns[0] == "__FB_CRON_JOB_SCHEDULE_DATETIME_0 0 * * *_UTC_None"
    assert job_datetime_columns[1] == "__FB_CRON_JOB_SCHEDULE_DATETIME_0 12 * * *_UTC_None"

    # The SQL expression should be valid and reference both columns correctly
    sql_str = str(result_expr)
    assert "job_schedule_1" in sql_str
    assert "job_schedule_2" in sql_str


def test_get_request_table_join_all_job_schedules_expr_empty_tables():
    """
    Test get_request_table_join_all_job_schedules_expr with empty job schedule table set
    """
    # Setup
    adapter = get_sql_adapter_from_source_type(SourceType.SNOWFLAKE)
    request_table_name = "REQUEST_TABLE"
    request_table_columns = ["col1", "col2", "point_in_time"]

    job_schedule_table_set = JobScheduleTableSet(tables=[])

    # Execute
    result_expr, job_datetime_columns = get_request_table_join_all_job_schedules_expr(
        request_table_name=request_table_name,
        request_table_columns=request_table_columns,
        job_schedule_table_set=job_schedule_table_set,
        adapter=adapter,
    )

    # Verify
    assert len(job_datetime_columns) == 0

    # The SQL should just be a simple select from the request table
    sql_str = str(result_expr)
    assert "REQUEST_TABLE" in sql_str
    assert "col1" in sql_str
    assert "col2" in sql_str
    assert "point_in_time" in sql_str
