"""
Lookup target operations
"""
from typing import Any

import pandas as pd
import pytest

from featurebyte.api.target import Target
from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
)
from tests.integration.api.lookup_operations_utils import (
    check_lookup_feature_or_target_is_time_aware,
)


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_lookup_features_dimension_view(dimension_view):
    """
    Test lookup targets with same column name
    """
    target_name = "ItemTypeFeatureDimensionView"
    dimension_target = dimension_view["item_type"].as_target(target_name)
    preview_params = {"POINT_IN_TIME": "2001-11-15 10:00:00", "item_id": "item_42"}
    target_preview_df = dimension_target.preview(pd.DataFrame([preview_params]))
    assert target_preview_df.iloc[0].to_dict() == {
        target_name: "type_42",
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_event_view_lookup_features(event_table, transaction_data_upper_case):
    """
    Test lookup features from EventView are time based
    """
    view = event_table.get_view()
    lookup_column_name = "ÀMOUNT"
    target_name = "lookup_target"
    target = view[lookup_column_name].as_target(target_name)
    check_lookup_feature_or_target_is_time_aware(
        feature_or_target=target,
        feature_or_target_name=target_name,
        df=transaction_data_upper_case,
        primary_key_column="TRANSACTION_ID",
        primary_key_value="T42",
        primary_key_serving_name="order_id",
        lookup_column_name=lookup_column_name,
        event_timestamp_column="ËVENT_TIMESTAMP",
    )


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_item_view_lookup_features(item_table, expected_joined_event_item_dataframe):
    """
    Test lookup features from ItemView are time based
    """
    view = item_table.get_view()
    lookup_column_name = "item_type"
    target_name = "lookup_target"
    target = view[lookup_column_name].as_target(target_name)
    check_lookup_feature_or_target_is_time_aware(
        feature_or_target=target,
        feature_or_target_name=target_name,
        df=expected_joined_event_item_dataframe,
        primary_key_column="item_id",
        primary_key_value="item_42",
        primary_key_serving_name="item_id",
        lookup_column_name=lookup_column_name,
        event_timestamp_column="ËVENT_TIMESTAMP",
    )


def _target_with_offset_test_helper(
    target: Target,
    target_name: str,
    lookup_column_name: str,
    df: pd.DataFrame,
    timestamp_column_name: str,
    primary_key_serving_name: str,
    primary_key_value: Any,
    primary_key_column: str,
    offset_duration: str,
):
    # Preview
    point_in_time = "2001-11-15 10:00:00"
    preview_params = {
        "POINT_IN_TIME": point_in_time,
        primary_key_serving_name: primary_key_value,
    }
    preview_output = target.preview(pd.DataFrame([preview_params])).iloc[0].to_dict()

    # Compare with expected result
    mask = (
        pd.to_datetime(df[timestamp_column_name], utc=True).dt.tz_localize(None)
        <= (pd.to_datetime(point_in_time) + pd.Timedelta(offset_duration))
    ) & (df[primary_key_column] == primary_key_value)
    expected_row = df[mask].sort_values(timestamp_column_name).iloc[-1]
    assert preview_output[target_name] == expected_row[lookup_column_name]


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_scd_lookup_target_with_offset(config, scd_table, scd_dataframe):
    """
    Test creating lookup target from a SCDView with offset
    """
    offset = "90d"
    scd_view = scd_table.get_view()
    target_name = "Current User Status Offset 90d"
    scd_lookup_target = scd_view["User Status"].as_target(target_name, offset=offset)
    _target_with_offset_test_helper(
        scd_lookup_target,
        target_name,
        "User Status",
        scd_dataframe,
        "Effective Timestamp",
        "üser id",
        1,
        "User ID",
        offset,
    )


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_event_lookup_target_with_offset(config, event_table, transaction_data_upper_case):
    """
    Test creating event target from a event view with offset
    """
    # Event lookup target
    offset = "90d"
    view = event_table.get_view()
    lookup_column_name = "ÀMOUNT"
    target_name = "lookup_target"
    timestamp_column = "ËVENT_TIMESTAMP"
    event_target = view[lookup_column_name].as_target(target_name, offset=offset)
    _target_with_offset_test_helper(
        event_target,
        target_name,
        lookup_column_name,
        transaction_data_upper_case,
        timestamp_column,
        "order_id",
        "T42",
        "TRANSACTION_ID",
        offset,
    )
