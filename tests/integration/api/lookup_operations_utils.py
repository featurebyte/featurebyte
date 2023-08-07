"""
Lookup operation test utils
"""
import numpy as np
import pandas as pd
import pytz


def check_lookup_feature_or_target_is_time_aware(
    feature_or_target,
    feature_or_target_name: str,
    df,
    primary_key_column,
    primary_key_value,
    primary_key_serving_name,
    lookup_column_name,
    event_timestamp_column,
):
    """
    Check that lookup feature is time based (value is NA is point in time is prior to event time)
    """
    lookup_row = df[df[primary_key_column] == primary_key_value].iloc[0]
    event_timestamp = lookup_row[event_timestamp_column].astimezone(pytz.utc).replace(tzinfo=None)

    # target = view[lookup_column_name].as_target("lookup_target")
    # Lookup from event should be time based - value is NA is point in time is prior to event time
    ts_before_event = event_timestamp - pd.Timedelta("7d")
    ts_after_event = event_timestamp + pd.Timedelta("7d")
    expected_feature_value_if_after_event = lookup_row[lookup_column_name]
    # Make sure to pick a non-na value to test meaningfully
    assert not pd.isna(expected_feature_value_if_after_event)

    # Point in time after event time - non-NA
    df = feature_or_target.preview(
        pd.DataFrame(
            [
                {
                    "POINT_IN_TIME": ts_after_event,
                    primary_key_serving_name: primary_key_value,
                }
            ]
        )
    )
    expected = pd.Series(
        {
            "POINT_IN_TIME": ts_after_event,
            primary_key_serving_name: primary_key_value,
            feature_or_target_name: expected_feature_value_if_after_event,
        }
    )
    pd.testing.assert_series_equal(df.iloc[0], expected, check_names=False)

    # Point in time before event time - NA
    df = feature_or_target.preview(
        pd.DataFrame(
            [
                {
                    "POINT_IN_TIME": ts_before_event,
                    primary_key_serving_name: primary_key_value,
                }
            ]
        )
    )
    expected = pd.Series(
        {
            "POINT_IN_TIME": ts_before_event,
            primary_key_serving_name: primary_key_value,
            feature_or_target_name: np.nan,
        }
    )
    pd.testing.assert_series_equal(df.iloc[0], expected, check_names=False)
