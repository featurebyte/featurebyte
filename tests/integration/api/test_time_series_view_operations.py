"""
Integration tests for TimeSeriesView operations
"""

from datetime import datetime
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from featurebyte import CalendarWindow, CronFeatureJobSetting, FeatureList, RequestColumn
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from tests.util.helper import fb_assert_frame_equal


@pytest.fixture(name="time_series_window_aggregate_feature")
def time_series_window_aggregate_feature_fixture(time_series_table):
    """
    Fixture for a feature derived from TimeSeriewView
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
    return feature


def test_times_series_view(time_series_table):
    """
    Test TimeSeriesView
    """
    view = time_series_table.get_view()
    view = view[view["series_id_col"] == "S0"]
    view = view[["reference_datetime_col", "series_id_col", "value_col"]]
    view["hour"] = view["reference_datetime_col"].dt.hour
    df_preview = view.preview(limit=10000)
    df_preview.sort_values("reference_datetime_col", inplace=True)
    actual = df_preview.iloc[:10].to_dict(orient="list")
    expected = {
        "reference_datetime_col": [
            "2001|01|01",
            "2001|01|02",
            "2001|01|03",
            "2001|01|04",
            "2001|01|05",
            "2001|01|06",
            "2001|01|07",
            "2001|01|08",
            "2001|01|09",
            "2001|01|10",
        ],
        "series_id_col": ["S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0"],
        "value_col": [0.0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09],
        "hour": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    }
    assert actual == expected


def test_aggregate_over(time_series_window_aggregate_feature):
    """
    Test TimeSeriesView
    """
    feature = time_series_window_aggregate_feature
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
    common_params = dict(
        value_column="value_col",
        windows=[CalendarWindow(unit="DAY", size=7)],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )

    feature_1 = view.groupby("series_id_col").aggregate_over(
        method="sum",
        **common_params,
        feature_names=["value_col_sum_7d"],
    )["value_col_sum_7d"]

    feature_2 = view.groupby("series_id_col").aggregate_over(
        method="latest", feature_names=["value_col_latest_7d"], **common_params
    )["value_col_latest_7d"]

    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-10 10:00:00"),
            "series_id": "S0",
        }
    ])
    feature_list = FeatureList([feature_1, feature_2], "test_feature_list")
    df_features = feature_list.compute_historical_features(preview_params)
    expected = preview_params.copy()
    # Point in time of "2001-01-10 10:00:00" UTC is "2001-01-10 18:00:00" Asia/Singapore, at
    # which point the last feature job is at "2001-01-10 10:00:00" Asia/Singapore. Hence, the
    # features should aggregate the values from "2001-01-03" to "2001-01-09" inclusive.
    expected["value_col_sum_7d"] = [0.35]
    expected["value_col_latest_7d"] = [0.08]
    fb_assert_frame_equal(df_features, expected)


def test_aggregate_over_post_aggregate_with_tz_column(time_series_table_tz_column):
    """
    Test TimeSeriesView aggregate_over using the latest method for reference_datetime_column with a
    timezone offset column
    """
    view = time_series_table_tz_column.get_view()
    latest_reference_datetime = view.groupby("series_id_col").aggregate_over(
        value_column="reference_datetime_col",
        method="latest",
        windows=[CalendarWindow(unit="DAY", size=7)],
        feature_names=["my_feature"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )["my_feature"]

    feature_1 = (RequestColumn.point_in_time() - latest_reference_datetime).dt.hour
    feature_1.name = "hour_since_latest_reference_datetime"

    feature_2 = (latest_reference_datetime - RequestColumn.point_in_time()).dt.hour
    feature_2.name = "hour_since_latest_reference_datetime_reversed"

    feature_3 = latest_reference_datetime.dt.hour
    feature_3.name = "hour_of_latest_reference_datetime"

    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-10 10:00:00"),
            "series_id_2": "S0",
        }
    ])
    feature_list = FeatureList([feature_1, feature_2, feature_3], "test_feature_list")
    df_features = feature_list.compute_historical_features(preview_params)
    expected = preview_params.copy()
    # Point in time of "2001-01-10 10:00:00" UTC is "2001-01-10 18:00:00" Asia/Singapore, at which
    # point the last feature job is at "2001-01-10 10:00:00" Asia/Singapore. Hence, the features
    # should aggregate the values from "2001-01-03" to "2001-01-09" inclusive. Latest reference
    # datetime during that period is "2001-01-09 00:00:00" Asia/Singapore, converted to UTC is
    # "2001-01-08 16:00:00". The difference with the point in time "2001-01-10 10:00:00" UTC is
    # 8 + 24 + 10 = 42 hours. Hour of the latest reference datetime should be in local time, which
    # is 0.
    expected["hour_since_latest_reference_datetime"] = [42]
    expected["hour_since_latest_reference_datetime_reversed"] = [-42]
    expected["hour_of_latest_reference_datetime"] = [0]
    fb_assert_frame_equal(df_features, expected)


def test_join_scd_view(time_series_table, scd_table_custom_date_format):
    """
    Test joining time series view with SCD view
    """
    view = time_series_table.get_view()
    scd_view = scd_table_custom_date_format.get_view()
    view = view.join(scd_view)

    # Check can preview
    df = view[view["user_id_col"] == 7].preview()
    assert df.iloc[0]["User Status"] == "STÀTUS_CODE_26"

    feature = view.groupby("series_id_col").aggregate_over(
        value_column="User Status",
        method="count_distinct",
        windows=[CalendarWindow(unit="DAY", size=7)],
        feature_names=["num_unique_user_status_7d"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )["num_unique_user_status_7d"]

    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-10 10:00:00"),
            "series_id": "S0",
        }
    ])
    feature_list = FeatureList([feature], "test_feature_list")
    df_features = feature_list.compute_historical_features(preview_params)
    expected = preview_params.copy()
    expected["num_unique_user_status_7d"] = [5]
    fb_assert_frame_equal(df_features, expected)


def test_deployment(config, time_series_window_aggregate_feature):
    """
    Test TimeSeriesView
    """
    feature = time_series_window_aggregate_feature
    feature_list = FeatureList([feature], "time_series_feature_production_ready")
    feature_list.save()
    deployment = feature_list.deploy(make_production_ready=True)
    with patch("featurebyte.service.feature_materialize.datetime", autospec=True) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 10, 12)
        deployment.enable()
    entity_serving_names = [
        {
            "series_id": "S0",
        }
    ]
    data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)
    client = config.get_client()
    with patch("featurebyte.service.online_serving.datetime", autospec=True) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 10, 12)
        with patch("croniter.croniter.timestamp_to_datetime") as mock_croniter:
            mock_croniter.return_value = datetime(2001, 1, 10, 10)
            res = client.post(
                f"/deployment/{deployment.id}/online_features",
                json=data.json_dict(),
            )

    assert res.status_code == 200
    df_feat = pd.DataFrame(res.json()["features"])
    df_expected = pd.DataFrame([{"series_id": "S0", "value_col_sum_7d": 0.35}])
    fb_assert_frame_equal(df_feat, df_expected)

    # when the forecast point is now, "value_col_sum_7d" value should be reset
    res = client.post(
        f"/deployment/{deployment.id}/online_features",
        json=data.json_dict(),
    )
    assert res.status_code == 200
    df_feat = pd.DataFrame(res.json()["features"])
    df_expected = pd.DataFrame([{"series_id": "S0", "value_col_sum_7d": np.nan}])
    fb_assert_frame_equal(df_feat, df_expected)
