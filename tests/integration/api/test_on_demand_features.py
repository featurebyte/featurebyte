"""
Integration tests for the on-demand features.
"""
import pandas as pd

from featurebyte.api.feature_group import FeatureGroup
from featurebyte.api.request_column import point_in_time
from tests.util.helper import fb_assert_frame_equal


def test_on_demand_feature_with_point_in_time(event_table):
    """
    Test on-demand feature with point in time
    """
    view = event_table.get_view()

    latest_timestamp = view.groupby("ÜSER ID").aggregate_over(
        value_column="ËVENT_TIMESTAMP",
        method="latest",
        windows=["90d"],
        feature_names=["latest_event_timestamp_90d"],
    )["latest_event_timestamp_90d"]

    feature = (point_in_time - latest_timestamp).dt.hour
    feature.name = "num_hour_since_last_event"

    feature_group = FeatureGroup([latest_timestamp, feature])

    preview_param = pd.DataFrame(
        [
            {
                "POINT_IN_TIME": "2001-01-02 10:00:00",
                "üser id": 1,
            }
        ]
    )
    df = feature_group.preview(preview_param)
    if df["latest_event_timestamp_90d"].dt.tz is not None:
        df["latest_event_timestamp_90d"] = df["latest_event_timestamp_90d"].dt.tz_localize(None)

    expected = pd.DataFrame(
        [
            {
                "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
                "üser id": 1,
                "latest_event_timestamp_90d": pd.Timestamp("2001-01-02 08:42:19.000673"),
                "num_hour_since_last_event": 1.294722,
            }
        ]
    )
    fb_assert_frame_equal(df, expected)
