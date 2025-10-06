"""
Integration tests for SnapshotsView
"""

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId

from featurebyte import (
    CalendarWindow,
    CronFeatureJobSetting,
    FeatureList,
    TimeInterval,
    TimestampSchema,
)
from tests.util.deployment import deploy_and_get_online_features
from tests.util.helper import (
    check_preview_and_compute_historical_features,
    fb_assert_frame_equal,
)


def test_snapshots_view(snapshots_table):
    """
    Test that SnapshotsView can be created and queried correctly
    """
    view = snapshots_table.get_view()
    view = view[view["series_id_col"] == "S0"]
    view = view[["snapshot_datetime_col", "series_id_col", "value_col"]]
    view["hour"] = view["snapshot_datetime_col"].dt.hour
    df_preview = view.preview(limit=10000)
    df_preview.sort_values("snapshot_datetime_col", inplace=True)
    actual = df_preview.iloc[:10].to_dict(orient="list")
    expected = {
        "snapshot_datetime_col": [
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


def test_snapshots_view_join_time_series_view(snapshots_table, time_series_table):
    """
    Test that SnapshotsView can be joined with TimeSeriesView correctly
    """
    snapshots_view = snapshots_table.get_view()
    time_series_view = time_series_table.get_view()
    joined_view = time_series_view.join(snapshots_view, rsuffix="_from_snapshots")
    joined_view = joined_view[joined_view["series_id_col"] == "S0"]
    df_preview = joined_view.preview(limit=10000)
    df_preview = df_preview.sort_values("reference_datetime_col")
    cols = [
        "reference_datetime_col",
        "series_id_col",
        "snapshot_datetime_col_from_snapshots",
        "value_col_from_snapshots",
    ]
    actual = df_preview[cols].iloc[-3:].to_dict(orient="list")
    expected = {
        "reference_datetime_col": ["2001|04|08", "2001|04|09", "2001|04|10"],
        "series_id_col": ["S0", "S0", "S0"],
        "snapshot_datetime_col_from_snapshots": ["2001|04|05", "2001|04|06", "2001|04|07"],
        "value_col_from_snapshots": [0.9400000000000001, 0.9500000000000001, 0.96],
    }
    assert actual == expected


def make_unique(name):
    """
    Make name unique by appending ObjectId
    """
    return f"{name}_{str(ObjectId())}"


@pytest.mark.asyncio
async def test_snapshots_view_join_snapshots_view_small(
    session,
    data_source,
    source_type,
    timestamp_format_string,
    timestamp_format_string_with_time,
):
    """
    Self-contained test that joins two small SnapshotsViews
    """
    df_left = pd.DataFrame({
        "snapshot_ts": ["2022|04|10", "2022|04|11", "2022|04|12"],
        "series_id": ["A", "A", "B"],
        "left_value": [10.0, 20.0, 30.0],
    })
    df_right = pd.DataFrame({
        "snapshot_ts": ["2022|04|10|00:00:00", "2022|04|12|00:00:00"],
        "series_id": ["A", "B"],
        "right_value": [1.0, 2.0],
    })

    table_prefix = make_unique(f"{source_type}_SNAPSHOTS_JOIN_SMALL").upper()
    left_table_name = f"{table_prefix}_LEFT"
    right_table_name = f"{table_prefix}_RIGHT"

    await session.register_table(left_table_name, df_left)
    await session.register_table(right_table_name, df_right)

    left_timestamp_schema = TimestampSchema(format_string=timestamp_format_string)
    right_timestamp_schema = TimestampSchema(format_string=timestamp_format_string_with_time)
    time_interval = TimeInterval(unit="DAY", value=1)

    left_source_table = data_source.get_source_table(
        table_name=left_table_name,
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    left_snapshots_table = left_source_table.create_snapshots_table(
        name=f"{table_prefix}_LEFT_TABLE",
        snapshot_datetime_column="snapshot_ts",
        snapshot_datetime_schema=left_timestamp_schema,
        time_interval=time_interval,
        series_id_column="series_id",
    )

    right_source_table = data_source.get_source_table(
        table_name=right_table_name,
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    right_snapshots_table = right_source_table.create_snapshots_table(
        name=f"{table_prefix}_RIGHT_TABLE",
        snapshot_datetime_column="snapshot_ts",
        snapshot_datetime_schema=right_timestamp_schema,
        time_interval=time_interval,
        series_id_column="series_id",
    )

    left_view = left_snapshots_table.get_view()
    right_view = right_snapshots_table.get_view()
    joined_view = left_view.join(right_view, rsuffix="_right")

    df_preview = joined_view.preview(limit=100)
    df_preview = df_preview.sort_values(by=["snapshot_ts", "series_id"]).reset_index(drop=True)

    expected = pd.DataFrame({
        "snapshot_ts": ["2022|04|10", "2022|04|11", "2022|04|12"],
        "series_id": ["A", "A", "B"],
        "left_value": [10.0, 20.0, 30.0],
        "snapshot_ts_right": ["2022|04|10|00:00:00", None, "2022|04|12|00:00:00"],
        "right_value_right": [1.0, None, 2.0],
    })
    assert set(df_preview.columns) == set(expected.columns)
    expected = expected[df_preview.columns]

    df_preview["snapshot_ts_right"] = df_preview["snapshot_ts_right"].replace({np.nan: None})
    fb_assert_frame_equal(
        df_preview,
        expected,
        sort_by_columns=["snapshot_ts", "series_id"],
    )


def test_aggregate_over(client, snapshots_table):
    """
    Test aggregate over on snapshots table
    """
    view = snapshots_table.get_view()
    feature_name = make_unique("value_col_sum_7d")
    feature = view.groupby("series_id_col").aggregate_over(
        value_column="value_col",
        method="sum",
        windows=[CalendarWindow(unit="DAY", size=7)],
        feature_names=[feature_name],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )[feature_name]
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-10 10:00:00"),
            "series_id": "S0",
        }
    ])
    feature_list = FeatureList([feature], str(ObjectId()))
    expected = preview_params.copy()
    expected[feature_name] = [0.35]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)
    online_features = deploy_and_get_online_features(
        client,
        feature_list,
        pd.Timestamp("2001-01-10 10:00:00"),
        [{"series_id": "S0"}],
    )
    fb_assert_frame_equal(online_features, expected.drop(["POINT_IN_TIME"], axis=1))


def test_lookup_features(client, snapshots_table):
    """
    Test that lookup features can be created from SnapshotsView
    """
    view = snapshots_table.get_view()
    feature_name = make_unique("snapshot_lookup_feature")
    lookup_feature = view["value_col"].as_feature(feature_name)
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": dt,
            "series_id": "S0",
        }
        for dt in pd.to_datetime(["2001-01-10 10:00:00", "2001-01-15 10:00:00"])
    ])
    feature_list = FeatureList([lookup_feature], str(ObjectId()))
    expected = preview_params.copy()
    expected[feature_name] = [0.06, 0.11]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)
    online_features = deploy_and_get_online_features(
        client,
        feature_list,
        pd.Timestamp("2001-01-10 10:00:00"),
        [{"series_id": "S0"}],
    )
    fb_assert_frame_equal(
        online_features.iloc[:1], expected.iloc[:1].drop(["POINT_IN_TIME"], axis=1)
    )


def test_lookup_target(snapshots_table):
    """
    Test that lookup target can be created from SnapshotsView
    """
    view = snapshots_table.get_view()
    lookup_target = view["value_col"].as_target(
        "snapshot_lookup_target",
        offset=3,
        fill_value=0,
    )
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": dt,
            "series_id": "S0",
        }
        for dt in pd.to_datetime(["2001-01-10 10:00:00", "2001-01-15 10:00:00"])
    ])

    expected = preview_params.copy()
    expected["snapshot_lookup_target"] = [0.12, 0.17]

    df_targets = lookup_target.compute_targets(preview_params)
    fb_assert_frame_equal(df_targets, expected, sort_by_columns=["POINT_IN_TIME"])


def test_aggregate_as_at_feature(client, snapshots_table):
    """
    Test that aggregate as at feature can be created from SnapshotsView
    """
    view = snapshots_table.get_view()
    feature_name = make_unique("value_col_by_user_id_asat")
    agg_feature = view.groupby("user_id_col").aggregate_asat(
        method="sum",
        value_column="value_col",
        feature_name=feature_name,
    )
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": dt,
            "üser id": 3,
        }
        for dt in pd.to_datetime(["2001-01-10 10:00:00", "2001-01-15 10:00:00"])
    ])
    feature_list = FeatureList([agg_feature], str(ObjectId()))
    expected = preview_params.copy()
    expected[feature_name] = [9.06, 5.11]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)
    online_features = deploy_and_get_online_features(
        client,
        feature_list,
        pd.Timestamp("2001-01-10 10:00:00"),
        [{"üser id": 3}],
    )
    fb_assert_frame_equal(
        online_features.iloc[:1], expected.iloc[:1].drop(["POINT_IN_TIME"], axis=1)
    )
