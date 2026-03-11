"""
Integration tests for TimeSeriesView operations
"""

from datetime import datetime
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import (
    CalendarWindow,
    Context,
    CronFeatureJobSetting,
    Entity,
    FeatureList,
    RequestColumn,
)
from featurebyte.enum import DBVarType, TimeIntervalUnit
from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from tests.util.helper import (
    check_preview_and_compute_historical_features,
    fb_assert_frame_equal,
)


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


@pytest.fixture(name="time_series_large_window_aggregate_feature")
def time_series_large_window_aggregate_feature_fixture(time_series_table):
    """
    Fixture for a feature derived from TimeSeriewView with a large feature window. This is not a
    realistic feature and is primarily used for testing deployment without requiring to mock
    datetimes.
    """
    view = time_series_table.get_view()
    feature = view.groupby("series_id_col").aggregate_over(
        value_column="value_col",
        method="sum",
        windows=[CalendarWindow(unit="YEAR", size=35)],
        feature_names=["value_col_sum_35y"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )["value_col_sum_35y"]
    return feature


@pytest_asyncio.fixture(name="batch_request_table")
async def batch_request_table_fixture(session, data_source):
    """
    Fixture for a BatchRequestTable
    """
    df = pd.DataFrame({"series_id": ["S0"]})
    table_name = f"request_table_{ObjectId()}"
    await session.register_table(table_name, df)
    source_table = data_source.get_source_table(
        table_name=table_name,
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    return source_table.create_batch_request_table(f"Batch Request Table ({ObjectId()})")


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
    expected = preview_params.copy()
    expected["value_col_sum_7d"] = [0.35]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)


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
    expected = preview_params.copy()
    expected["value_col_sum_7d_offset_1d"] = [0.28]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)


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
    expected = preview_params.copy()
    expected["value_col_sum_1month"] = [4.65]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)


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
    expected = preview_params.copy()
    # Point in time of "2001-01-10 10:00:00" UTC is "2001-01-10 18:00:00" Asia/Singapore, at
    # which point the last feature job is at "2001-01-10 10:00:00" Asia/Singapore. Hence, the
    # features should aggregate the values from "2001-01-03" to "2001-01-09" inclusive.
    expected["value_col_sum_7d"] = [0.35]
    expected["value_col_latest_7d"] = [0.08]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)


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
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)


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
    expected = preview_params.copy()
    expected["num_unique_user_status_7d"] = [5]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)


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


def test_batch_features_from_deployment(
    time_series_large_window_aggregate_feature, batch_request_table
):
    """
    Test batch feature table from deployment
    """
    feature = time_series_large_window_aggregate_feature
    feature_list = FeatureList([feature], str(ObjectId()))
    feature_list.save()
    deployment = feature_list.deploy(make_production_ready=True)
    deployment.enable()
    batch_feature_table = deployment.compute_batch_feature_table(
        batch_request_table,
        str(ObjectId()),
        point_in_time="2025-01-01 10:00:00Z",
    )
    df_batch_feature_table = batch_feature_table.to_pandas()
    df_expected = pd.DataFrame([
        {
            "series_id": "S0",
            "value_col_sum_35y": 49.5,
            "POINT_IN_TIME": pd.Timestamp("2025-01-01 10:00:00"),
        }
    ])
    fb_assert_frame_equal(df_batch_feature_table, df_expected)


def test_time_series_view_join_scd_view(time_series_table, scd_table):
    """
    Test joining TimeSeriesView with an SCDView
    """
    time_series_view = time_series_table.get_view()
    scd_view = scd_table.get_view()
    joined_view = time_series_view.join(scd_view)
    # Preview should work without errors
    df = joined_view[joined_view["series_id_col"] == "S1"].preview()
    assert df["User Status"].nunique() > 0


def test_multiple_features(time_series_table):
    """
    Test TimeSeriesView
    """
    view = time_series_table.get_view()

    features = []
    for method in ["sum", "min", "max"]:
        feature = view.groupby("series_id_col").aggregate_over(
            value_column="value_col",
            method=method,
            windows=[CalendarWindow(unit="MONTH", size=3)],
            feature_names=[f"value_col_{method}_3m"],
            feature_job_setting=CronFeatureJobSetting(
                crontab="0 8 * * *",
                timezone="Asia/Singapore",
            ),
        )[f"value_col_{method}_3m"]
        features.append(feature)
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-02-10 10:00:00"),
            "series_id": "S0",
        }
    ])
    feature_list = FeatureList(features, "test_feature_list")
    expected = preview_params.copy()
    expected["value_col_sum_3m"] = [4.65]
    expected["value_col_min_3m"] = [0.0]
    expected["value_col_max_3m"] = [0.3]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)


def test_blind_spot(time_series_table):
    """
    Test blind spot in calendar aggregation
    """
    view = time_series_table.get_view()
    feature_1 = view.groupby("series_id_col").aggregate_over(
        value_column="value_col",
        method="sum",
        windows=[CalendarWindow(unit="DAY", size=7)],
        feature_names=["feature_blind_spot_none"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )["feature_blind_spot_none"]
    feature_2 = view.groupby("series_id_col").aggregate_over(
        value_column="value_col",
        method="sum",
        windows=[CalendarWindow(unit="DAY", size=7)],
        feature_names=["feature_blind_spot_3d"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
            blind_spot=CalendarWindow(unit="DAY", size=3),
        ),
    )["feature_blind_spot_3d"]
    features = [feature_1, feature_2]
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": dt,
            "series_id": "S0",
        }
        for dt in pd.date_range("2001-02-10 10:00:00", "2001-02-20 10:00:00")
    ])
    feature_list = FeatureList(features, "test_feature_list")
    expected = preview_params.copy()
    expected["feature_blind_spot_none"] = [
        2.52,
        2.59,
        2.66,
        2.73,
        2.8,
        2.87,
        2.94,
        3.01,
        3.08,
        3.15,
        3.22,
    ]
    expected["feature_blind_spot_3d"] = [
        2.31,
        2.38,
        2.45,
        2.52,
        2.59,
        2.66,
        2.73,
        2.8,
        2.87,
        2.94,
        3.01,
    ]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)


def test_forward_aggregate_asat(time_series_table):
    """
    Test that forward_aggregate_asat target can be created from TimeSeriesView
    """
    view = time_series_table.get_view()
    target = view.groupby("user_id_col").forward_aggregate_asat(
        value_column="value_col",
        method="sum",
        target_name="ts_forward_asat_target",
        offset=3,
        fill_value=0.0,
    )
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": dt,
            "üser id": 3,
        }
        for dt in pd.to_datetime(["2001-01-10 10:00:00", "2001-01-15 10:00:00"])
    ])
    df_targets = target.compute_targets(preview_params)
    expected = preview_params.copy()
    # offset=3 shifts the lookup 3 days forward from each POINT_IN_TIME:
    # - 2001-01-10 + 3d = 2001-01-13: user_id=3 has value 1.12 (S1)
    # - 2001-01-15 + 3d = 2001-01-18: user_id=3 has no data, fill_value=0.0
    expected["ts_forward_asat_target"] = [1.12, 0.0]
    fb_assert_frame_equal(df_targets, expected)


@pytest.mark.asyncio
async def test_lookup_target(
    time_series_table, session, data_source, series_entity, timestamp_format_string
):
    """
    Test that lookup target can be created from TimeSeriesView and computed with forecast point.

    This test verifies that when an observation table is linked to a context with forecast_point_schema,
    the FORECAST_POINT column is used as the temporal reference for target computation.
    """
    view = time_series_table.get_view()
    lookup_target = view["value_col"].as_target(
        "time_series_lookup_target",
        offset=3,
        fill_value=0,
    )

    # Create a Context with forecast_point_schema
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        is_utc_time=True,
        format_string=timestamp_format_string,
    )
    forecast_context = Context.create(
        name=f"forecast_context_{ObjectId()}",
        primary_entity=[series_entity.name],
        forecast_point_schema=forecast_schema,
    )

    # Create observation set with FORECAST_POINT different from POINT_IN_TIME.
    # The FORECAST_POINT determines which reference_datetime row to look up.
    # With offset=3, the target looks up reference_datetime = FORECAST_POINT + 3 days.
    # For FORECAST_POINT = "2001-01-07", the target should look up "2001-01-10" -> value 0.09
    # For FORECAST_POINT = "2001-01-12", the target should look up "2001-01-15" -> value 0.14
    df_obs = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-01 00:00:00"),
            "series_id": "S0",
            "FORECAST_POINT": "2001|01|07",
        },
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-01 00:00:00"),
            "series_id": "S0",
            "FORECAST_POINT": "2001|01|12",
        },
    ])

    # Register the dataframe and create observation table linked to the forecast context
    unique_id = ObjectId()
    db_table_name = f"df_{unique_id}"
    await session.register_table(db_table_name, df_obs)
    source_table = data_source.get_source_table(
        db_table_name,
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    observation_table = source_table.create_observation_table(
        f"obs_table_forecast_{unique_id}",
        context_name=forecast_context.name,
    )

    # Compute targets using the observation table (which triggers the forecast_point path)
    df_targets = lookup_target.compute_targets(observation_table)

    # Verify the target was computed using FORECAST_POINT as temporal reference.
    # With offset=3 and the reference dates, we expect:
    # - FORECAST_POINT "2001-01-07" + offset 3 days -> looks up "2001-01-10" -> value 0.09
    # - FORECAST_POINT "2001-01-12" + offset 3 days -> looks up "2001-01-15" -> value 0.14
    assert len(df_targets) == 2
    df_targets_sorted = df_targets.sort_values("FORECAST_POINT").reset_index(drop=True)
    assert df_targets_sorted["time_series_lookup_target"].tolist() == [0.09, 0.14]


@pytest.mark.asyncio
async def test_lookup_target_with_composite_series_id(
    session, data_source, catalog, timestamp_format_string
):
    """
    Test that lookup target can be created from a TimeSeriesView with composite series_id_columns
    and computed correctly using an observation table with a ForecastPointSchema context.

    The table tracks daily sales per (store, sku) pair. Two stores (STORE_1, STORE_2) each
    carry two SKUs (SKU_A, SKU_B), so neither store alone nor sku alone uniquely identifies
    a series. The composite key (store, sku) is required for a unique lookup.

    Data layout (daily, Jan 5–7; day 7 is the target lookup date):
      store   | sku   | date       | value
      --------|-------|------------|-------
      STORE_1 | SKU_A | 2001-01-07 | 70.0   <- target row for observation 1
      STORE_1 | SKU_B | 2001-01-07 | 700.0  <- target row for observation 2
      STORE_2 | SKU_A | 2001-01-07 | 71.0   <- false match if only sku="SKU_A" were used
      STORE_2 | SKU_B | 2001-01-07 | 701.0  <- false match if only sku="SKU_B" were used

    POINT_IN_TIME (2001-01-01) is before FORECAST_POINT (2001-01-07), reflecting a realistic
    forecasting scenario. The target looks up the value directly at FORECAST_POINT:
      (STORE_1, SKU_A) -> 70.0
      (STORE_1, SKU_B) -> 700.0
      (STORE_2, SKU_A) -> 71.0
      (STORE_2, SKU_B) -> 701.0
    """
    _ = catalog

    unique_id = ObjectId()
    suffix = unique_id.binary.hex()[:8].upper()

    # Only the composite key (store, sku) uniquely identifies each series.
    # ts column is a string formatted per timestamp_format_string (e.g. "2001|01|07").
    df = pd.DataFrame([
        {"store": "STORE_1", "sku": "SKU_A", "ts": "2001|01|05", "val": 50.0},
        {"store": "STORE_1", "sku": "SKU_A", "ts": "2001|01|06", "val": 60.0},
        {"store": "STORE_1", "sku": "SKU_A", "ts": "2001|01|07", "val": 70.0},
        {"store": "STORE_1", "sku": "SKU_B", "ts": "2001|01|05", "val": 500.0},
        {"store": "STORE_1", "sku": "SKU_B", "ts": "2001|01|06", "val": 600.0},
        {"store": "STORE_1", "sku": "SKU_B", "ts": "2001|01|07", "val": 700.0},
        {"store": "STORE_2", "sku": "SKU_A", "ts": "2001|01|05", "val": 51.0},
        {"store": "STORE_2", "sku": "SKU_A", "ts": "2001|01|06", "val": 61.0},
        {"store": "STORE_2", "sku": "SKU_A", "ts": "2001|01|07", "val": 71.0},
        {"store": "STORE_2", "sku": "SKU_B", "ts": "2001|01|05", "val": 501.0},
        {"store": "STORE_2", "sku": "SKU_B", "ts": "2001|01|06", "val": 601.0},
        {"store": "STORE_2", "sku": "SKU_B", "ts": "2001|01|07", "val": 701.0},
    ])

    table_name = f"COMPOSITE_SERIES_TS_{suffix}"
    await session.register_table(table_name, df)

    source_table = data_source.get_source_table(
        table_name,
        database_name=session.database_name,
        schema_name=session.schema_name,
    )

    # Create two separate entities for the composite key columns
    entity_store = Entity(name=f"CompositeStore_{suffix}", serving_names=[f"store_{suffix}"])
    entity_store.save()
    entity_sku = Entity(name=f"CompositeSKU_{suffix}", serving_names=[f"sku_{suffix}"])
    entity_sku.save()

    # Create TimeSeriesTable with composite series_id_columns (ts column is a string
    # formatted per timestamp_format_string, so format_string is required)
    ts_table = source_table.create_time_series_table(
        name=f"composite_series_ts_{suffix}",
        series_id_columns=["store", "sku"],
        reference_datetime_column="ts",
        reference_datetime_schema=TimestampSchema(
            format_string=timestamp_format_string, is_utc_time=True
        ),
        time_interval=TimeInterval(unit=TimeIntervalUnit.DAY, value=1),
    )
    ts_table["store"].as_entity(entity_store.name)
    ts_table["sku"].as_entity(entity_sku.name)

    view = ts_table.get_view()
    lookup_target = view["val"].as_target(
        f"composite_lookup_target_{suffix}",
        fill_value=0,
    )

    # Create a Context with forecast_point_schema and composite primary entities.
    # The FORECAST_POINT column will be used as the temporal reference for target computation.
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        is_utc_time=True,
        format_string=timestamp_format_string,
    )
    forecast_context = Context.create(
        name=f"forecast_context_composite_{unique_id}",
        primary_entity=[entity_store.name, entity_sku.name],
        forecast_point_schema=forecast_schema,
    )

    # One observation per (store, sku) combination. POINT_IN_TIME (2001-01-01) is before
    # FORECAST_POINT (2001-01-07). The target looks up the value directly at FORECAST_POINT.
    # Using store alone would return two SKUs per store; using sku alone would return two stores
    # per SKU — only the composite key uniquely identifies each row.
    target_col = f"composite_lookup_target_{suffix}"
    df_obs = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-01 00:00:00"),
            f"store_{suffix}": "STORE_1",
            f"sku_{suffix}": "SKU_A",
            "FORECAST_POINT": "2001|01|07",
            target_col: 70.0,
        },
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-01 00:00:00"),
            f"store_{suffix}": "STORE_1",
            f"sku_{suffix}": "SKU_B",
            "FORECAST_POINT": "2001|01|07",
            target_col: 700.0,
        },
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-01 00:00:00"),
            f"store_{suffix}": "STORE_2",
            f"sku_{suffix}": "SKU_A",
            "FORECAST_POINT": "2001|01|07",
            target_col: 71.0,
        },
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-01 00:00:00"),
            f"store_{suffix}": "STORE_2",
            f"sku_{suffix}": "SKU_B",
            "FORECAST_POINT": "2001|01|07",
            target_col: 701.0,
        },
    ])
    obs_table_name = f"COMPOSITE_OBS_{suffix}"
    await session.register_table(obs_table_name, df_obs.drop(columns=[target_col]))
    obs_source_table = data_source.get_source_table(
        obs_table_name,
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    observation_table = obs_source_table.create_observation_table(
        f"obs_composite_{unique_id}",
        context_name=forecast_context.name,
    )

    df_targets = lookup_target.compute_targets(observation_table)

    fb_assert_frame_equal(
        df_targets,
        df_obs,
        sort_by_columns=[f"store_{suffix}", f"sku_{suffix}"],
    )
