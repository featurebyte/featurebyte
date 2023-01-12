"""
Integration tests related to DimensionView
"""
import pandas as pd

from featurebyte import EventView
from featurebyte.api.dimension_view import DimensionView


def test_dimension_lookup_features(dimension_data):
    """
    Test lookup features from DimensionView
    """
    dimension_view = DimensionView.from_dimension_data(dimension_data)

    # Test single lookup feature
    feature = dimension_view["item_type"].as_feature("ItemTypeFeature")
    df = feature.preview({"POINT_IN_TIME": "2001-11-15 10:00:00", "item_id": "item_42"})
    assert df.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
        "item_id": "item_42",
        "ItemTypeFeature": "type_42",
    }

    # Test multiple lookup features
    feature_group = dimension_view.as_features(
        column_names=["item_name", "item_type"],
        feature_names=["ItemNameFeature", "ItemTypeFeature"],
    )
    df = feature_group.preview({"POINT_IN_TIME": "2001-11-15 10:00:00", "item_id": "item_42"})
    assert df.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
        "item_id": "item_42",
        "ItemNameFeature": "name_42",
        "ItemTypeFeature": "type_42",
    }


def test_is_in_dictionary(dimension_data, event_data):
    """
    Test is in dictionary
    """
    # get lookup feature
    dimension_view = DimensionView.from_dimension_data(dimension_data)
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
    isin_feature = lookup_feature.isin(dictionary_feature)

    # assert
    timestamp_str = "2001-01-13 12:00:00"
    isin_feature_preview = isin_feature.preview(
        {"POINT_IN_TIME": timestamp_str, "PRODUCT_ACTION": "purchase"},
    )
    assert isin_feature_preview.shape[0] == 1
