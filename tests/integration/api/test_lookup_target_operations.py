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
