"""
Lookup feature operations
"""
import numpy as np
import pandas as pd
import pytest
import pytz

from featurebyte import EventView, ItemView
from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_lookup_features_same_column_name(dimension_view, item_data):
    """
    Test lookup features with same column name
    """
    # create lookup feature A
    dimension_feature = dimension_view["item_type"].as_feature("ItemTypeFeatureDimensionView")

    # create lookup feature B from different table, but has same column name
    item_view = ItemView.from_item_data(item_data)
    item_feature = item_view["item_type"].as_feature("ItemTypeFeatureItemView")

    # create lookup feature C using A and B
    new_feature = dimension_feature == item_feature
    new_feature.name = "item_type_matches"

    # assert
    preview_params = {"POINT_IN_TIME": "2001-11-15 10:00:00", "item_id": "item_42"}
    new_feature_preview = new_feature.preview(pd.DataFrame([preview_params]))
    assert new_feature_preview.iloc[0].to_dict() == {
        new_feature.name: False,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


def check_lookup_feature_is_time_aware(
    view,
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

    # Create lookup feature
    feature = view[lookup_column_name].as_feature("lookup_feature")
    # Lookup from event should be time based - value is NA is point in time is prior to event time
    ts_before_event = event_timestamp - pd.Timedelta("7d")
    ts_after_event = event_timestamp + pd.Timedelta("7d")
    expected_feature_value_if_after_event = lookup_row[lookup_column_name]
    # Make sure to pick a non-na value to test meaningfully
    assert not pd.isna(expected_feature_value_if_after_event)

    # Point in time after event time - non-NA
    df = feature.preview(
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
            "lookup_feature": expected_feature_value_if_after_event,
        }
    )
    pd.testing.assert_series_equal(df.iloc[0], expected, check_names=False)

    # Point in time before event time - NA
    df = feature.preview(
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
            "lookup_feature": np.nan,
        }
    )
    pd.testing.assert_series_equal(df.iloc[0], expected, check_names=False)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_event_view_lookup_features(event_data, transaction_data_upper_case):
    """
    Test lookup features from EventView are time based
    """
    check_lookup_feature_is_time_aware(
        view=EventView.from_event_data(event_data),
        df=transaction_data_upper_case,
        primary_key_column="TRANSACTION_ID",
        primary_key_value="T42",
        primary_key_serving_name="order_id",
        lookup_column_name="ÀMOUNT",
        event_timestamp_column="ËVENT_TIMESTAMP",
    )


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_item_view_lookup_features(item_data, expected_joined_event_item_dataframe):
    """
    Test lookup features from ItemView are time based
    """
    check_lookup_feature_is_time_aware(
        view=ItemView.from_item_data(item_data),
        df=expected_joined_event_item_dataframe,
        primary_key_column="item_id",
        primary_key_value="item_42",
        primary_key_serving_name="item_id",
        lookup_column_name="item_type",
        event_timestamp_column="ËVENT_TIMESTAMP",
    )
