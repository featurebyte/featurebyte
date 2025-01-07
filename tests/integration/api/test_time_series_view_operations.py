"""
Integration tests for TimeSeriesView operations
"""

import pandas as pd

from featurebyte import CalendarWindow, CronFeatureJobSetting, FeatureList
from tests.util.helper import fb_assert_frame_equal


def test_times_series_view(time_series_table):
    """
    Test TimeSeriesView
    """
    view = time_series_table.get_view()
    view = view[view["series_id_col"] == "S0"]
    df_preview = view.preview()
    actual = df_preview.to_dict(orient="list")
    expected = {
        "reference_datetime_col": [
            "20010101",
            "20010102",
            "20010103",
            "20010104",
            "20010105",
            "20010106",
            "20010107",
            "20010108",
            "20010109",
            "20010110",
        ],
        "series_id_col": ["S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0"],
        "value_col": [0.0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09],
    }
    assert actual == expected


def test_aggregate_over(time_series_table):
    """
    Test TimeSeriesView
    """
    view = time_series_table.get_view()
    feature = view.groupby("series_id_col").aggregate_over(
        value_column="value_col",
        method="sum",
        windows=[CalendarWindow(unit="DAY", size=7)],
        feature_names=["value_col_sum_7d"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )["value_col_sum_7d"]
    # Point in time of "2001-01-10 10:00:00" UTC is "2001-01-10 18:00:00" Asia/Singapore, at
    # which point the last feature job is at "2001-01-10 10:00:00" Asia/Singapore. Hence, this
    # feature should sum the values from "2001-01-03" to "2001-01-09" inclusive.
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-10 10:00:00"),
            "series_id": "S0",
        }
    ])
    feature_list = FeatureList([feature], "test_feature_list")
    df_features = feature_list.compute_historical_features(preview_params)
    expected = preview_params.copy()
    expected["value_col_sum_7d"] = [0.35]
    fb_assert_frame_equal(df_features, expected)


def test_aggregate_over_offset(time_series_table):
    """
    Test TimeSeriesView
    """
    view = time_series_table.get_view()
    feature = view.groupby("series_id_col").aggregate_over(
        value_column="value_col",
        method="sum",
        windows=[CalendarWindow(unit="DAY", size=7)],
        offset=CalendarWindow(unit="DAY", size=1),
        feature_names=["value_col_sum_7d_offset_1d"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )["value_col_sum_7d_offset_1d"]
    # Point in time of "2001-01-10 10:00:00" UTC is "2001-01-10 18:00:00" Asia/Singapore, at which
    # point the last feature job is at "2001-01-10 10:00:00" Asia/Singapore. In addition, an offset
    # of 1 day is applied. Hence, this feature should sum the values from "2001-01-02" to
    # "2001-01-08" inclusive.
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-10 10:00:00"),
            "series_id": "S0",
        }
    ])
    feature_list = FeatureList([feature], "test_feature_list")
    df_features = feature_list.compute_historical_features(preview_params)
    expected = preview_params.copy()
    expected["value_col_sum_7d_offset_1d"] = [0.28]
    fb_assert_frame_equal(df_features, expected)


def test_aggregate_over_month(time_series_table):
    """
    Test TimeSeriesView
    """
    view = time_series_table.get_view()
    feature = view.groupby("series_id_col").aggregate_over(
        value_column="value_col",
        method="sum",
        windows=[CalendarWindow(unit="MONTH", size=1)],
        feature_names=["value_col_sum_1month"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )["value_col_sum_1month"]
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-02-10 10:00:00"),
            "series_id": "S0",
        }
    ])
    feature_list = FeatureList([feature], "test_feature_list")
    df_features = feature_list.compute_historical_features(preview_params)
    expected = preview_params.copy()
    expected["value_col_sum_1month"] = [4.65]
    fb_assert_frame_equal(df_features, expected)


def test_aggregate_over_latest(time_series_table):
    """
    Test TimeSeriesView aggregate_over using the latest method
    """
    view = time_series_table.get_view()
    feature = view.groupby("series_id_col").aggregate_over(
        value_column="value_col",
        method="latest",
        windows=[CalendarWindow(unit="DAY", size=7)],
        feature_names=["value_col_latest_7d"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )["value_col_latest_7d"]
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-10 10:00:00"),
            "series_id": "S0",
        }
    ])
    feature_list = FeatureList([feature], "test_feature_list")
    df_features = feature_list.compute_historical_features(preview_params)
    expected = preview_params.copy()
    expected["value_col_latest_7d"] = [0.08]
    fb_assert_frame_equal(df_features, expected)
