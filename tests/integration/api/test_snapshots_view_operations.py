"""
Integration tests for SnapshotsView
"""

import pandas as pd

from featurebyte import FeatureList
from tests.util.helper import (
    check_preview_and_compute_historical_features,
    fb_assert_frame_equal,
)


def test_snapshots_view(snapshots_table):
    """
    Test that SnapshotsView can be created and queried correctly
    """
    view = snapshots_table.get_view()
    view = view[view["snapshot_id_col"] == "S0"]
    view = view[["snapshot_datetime_col", "snapshot_id_col", "value_col"]]
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
        "snapshot_id_col": ["S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0"],
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


def test_lookup_features(snapshots_table):
    """
    Test that lookup features can be created from SnapshotsView
    """
    view = snapshots_table.get_view()
    lookup_feature = view["value_col"].as_feature("snapshot_lookup_feature")
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": dt,
            "series_id": "S0",
        }
        for dt in pd.to_datetime(["2001-01-10 10:00:00", "2001-01-15 10:00:00"])
    ])
    feature_list = FeatureList([lookup_feature], "test_feature_list")
    expected = preview_params.copy()
    expected["snapshot_lookup_feature"] = [0.06, 0.11]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)


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
    expected["snapshot_lookup_target"] = [0.09, 0.14]

    df_targets = lookup_target.compute_targets(preview_params)
    fb_assert_frame_equal(df_targets, expected, sort_by_columns=["POINT_IN_TIME"])


def test_aggregate_as_at_feature(snapshots_table):
    """
    Test that aggregate as at feature can be created from SnapshotsView
    """
    view = snapshots_table.get_view()
    agg_feature = view.groupby("user_id_col").aggregate_asat(
        method="sum",
        value_column="value_col",
        feature_name="value_col_by_user_id_asat",
    )
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": dt,
            "üser id": 3,
        }
        for dt in pd.to_datetime(["2001-01-10 10:00:00", "2001-01-15 10:00:00"])
    ])
    feature_list = FeatureList([agg_feature], "test_feature_list")
    expected = preview_params.copy()
    expected["value_col_by_user_id_asat"] = [9.06, 5.11]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)
