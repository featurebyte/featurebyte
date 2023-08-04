"""
Lookup target operations
"""
import pandas as pd
import pytest

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


@pytest.mark.parametrize("source_type", ["databricks", "snowflake", "spark"], indirect=True)
def test_scd_lookup_target_with_offset(config, scd_table, scd_dataframe):
    """
    Test creating lookup target from a SCDView with offset
    """
    # SCD lookup target
    offset = "90d"
    scd_view = scd_table.get_view()
    scd_lookup_target = scd_view["User Status"].as_target(
        "Current User Status Offset 90d", offset=offset
    )

    # Preview
    point_in_time = "2001-11-15 10:00:00"
    user_id = 1
    preview_params = {
        "POINT_IN_TIME": point_in_time,
        "üser id": user_id,
    }
    preview_output = scd_lookup_target.preview(pd.DataFrame([preview_params])).iloc[0].to_dict()

    # Compare with expected result
    mask = (
        pd.to_datetime(scd_dataframe["Effective Timestamp"], utc=True).dt.tz_localize(None)
        <= (pd.to_datetime(point_in_time) + pd.Timedelta(offset))
    ) & (scd_dataframe["User ID"] == user_id)
    expected_row = scd_dataframe[mask].sort_values("Effective Timestamp").iloc[-1]
    assert preview_output["Current User Status Offset 90d"] == expected_row["User Status"]
