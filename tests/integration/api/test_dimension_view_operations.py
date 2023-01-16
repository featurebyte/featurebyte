"""
Integration tests related to DimensionView
"""

from featurebyte import EventView
from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
)


def test_dimension_lookup_features(dimension_view):
    """
    Test lookup features from DimensionView
    """
    # Test single lookup feature
    feature = dimension_view["item_type"].as_feature("ItemTypeFeature")
    preview_params = {"POINT_IN_TIME": "2001-11-15 10:00:00", "item_id": "item_42"}
    df = feature.preview(preview_params)
    assert df.iloc[0].to_dict() == {
        "ItemTypeFeature": "type_42",
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }

    # Test multiple lookup features
    feature_group = dimension_view.as_features(
        column_names=["item_name", "item_type"],
        feature_names=["ItemNameFeature", "ItemTypeFeature"],
    )
    df = feature_group.preview(preview_params)
    assert df.iloc[0].to_dict() == {
        "ItemNameFeature": "name_42",
        "ItemTypeFeature": "type_42",
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


def test_is_in_dictionary__target_is_dictionary_feature(dimension_view, event_data):
    """
    Test is in dictionary
    """
    # get lookup feature
    lookup_feature = dimension_view["item_type"].as_feature("ItemTypeFeature")

    # get dictionary feature
    event_view = EventView.from_event_data(event_data)
    feature_group = event_view.groupby("CUST_ID", category="USER ID").aggregate_over(
        value_column="PRODUCT_ACTION",
        method="latest",
        windows=["30d"],
        feature_names=["LATEST_ACTION_DICT_30d"],
    )
    dictionary_feature = feature_group["LATEST_ACTION_DICT_30d"]

    # perform is in
    isin_feature = lookup_feature.isin(dictionary_feature)
    isin_feature.name = "lookup_is_in_dictionary"

    # assert
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "cust_id": "1", "item_id": "item_0"}
    isin_feature_preview = isin_feature.preview(preview_params)
    assert isin_feature_preview.shape[0] == 1
    assert isin_feature_preview.iloc[0].to_dict() == {
        "lookup_is_in_dictionary": False,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }


def test_is_in_dictionary__target_is_array(dimension_view):
    """
    Test is in array
    """
    lookup_feature = dimension_view["item_type"].as_feature("ItemTypeFeature")

    # perform is in check
    isin_feature = lookup_feature.isin(["type_0", "type_1"])
    isin_feature.name = "lookup_is_in_dictionary"

    # try to get preview and assert
    preview_params = {"POINT_IN_TIME": "2001-01-13 12:00:00", "item_id": "item_0"}
    isin_feature_preview = isin_feature.preview(preview_params)
    assert isin_feature_preview.shape[0] == 1
    assert isin_feature_preview.iloc[0].to_dict() == {
        "lookup_is_in_dictionary": True,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }
