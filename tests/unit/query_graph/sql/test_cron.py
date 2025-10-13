"""
Tests for featurebyte/query_graph/sql/cron.py
"""

from featurebyte import CronFeatureJobSetting, Crontab
from featurebyte.query_graph.sql.cron import (
    get_cron_feature_job_settings,
    get_request_table_job_datetime_column_name,
)


def test_get_cron_feature_job_settings(global_graph, time_series_window_aggregate_feature_node):
    """
    Test get_cron_feature_job_settings
    """
    cron_feature_job_settings = get_cron_feature_job_settings(
        global_graph,
        [time_series_window_aggregate_feature_node, time_series_window_aggregate_feature_node],
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
    )
    assert table_name == "__FB_CRON_JOB_SCHEDULE_DATETIME_0 0 * * *_Asia/Singapore_Asia/Tokyo"


def test_get_cron_feature_job_settings__blind_spot_handling(
    global_graph,
    time_series_window_aggregate_feature_node,
    time_series_window_aggregate_with_blind_spot_feature_node,
):
    """
    Test get_cron_feature_job_settings
    """
    cron_feature_job_settings = get_cron_feature_job_settings(
        global_graph,
        [
            time_series_window_aggregate_feature_node,
            time_series_window_aggregate_with_blind_spot_feature_node,
        ],
    )
    assert cron_feature_job_settings == [
        CronFeatureJobSetting(
            crontab=Crontab(minute=0, hour=0, day_of_month="*", month_of_year="*", day_of_week="*"),
            timezone="Etc/UTC",
            reference_timezone="Asia/Singapore",
        )
    ]
