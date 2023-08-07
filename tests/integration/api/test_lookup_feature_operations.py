"""
Lookup feature operations
"""
import pandas as pd
import pytest

from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
)
from tests.integration.api.lookup_operations_utils import (
    check_lookup_feature_or_target_is_time_aware,
)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_lookup_features_same_column_name(dimension_view, item_table):
    """
    Test lookup features with same column name
    """
    # create lookup feature A
    dimension_feature = dimension_view["item_type"].as_feature("ItemTypeFeatureDimensionView")

    # create lookup feature B from different table, but has same column name
    item_view = item_table.get_view()
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


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_event_view_lookup_features(event_table, transaction_data_upper_case):
    """
    Test lookup features from EventView are time based
    """
    view = event_table.get_view()
    lookup_column_name = "ÀMOUNT"
    feature_name = "lookup_feature"
    feature = view[lookup_column_name].as_feature(feature_name)
    check_lookup_feature_or_target_is_time_aware(
        feature_or_target=feature,
        feature_or_target_name=feature_name,
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
    feature_name = "lookup_feature"
    feature = view[lookup_column_name].as_feature(feature_name)
    check_lookup_feature_or_target_is_time_aware(
        feature_or_target=feature,
        feature_or_target_name=feature_name,
        df=expected_joined_event_item_dataframe,
        primary_key_column="item_id",
        primary_key_value="item_42",
        primary_key_serving_name="item_id",
        lookup_column_name=lookup_column_name,
        event_timestamp_column="ËVENT_TIMESTAMP",
    )
