"""
Integration tests related to DimensionView
"""
import pandas as pd

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
    feature_group = dimension_view[["item_name", "item_type"]].as_features(
        ["ItemNameFeature", "ItemTypeFeature"]
    )
    df = feature_group.preview({"POINT_IN_TIME": "2001-11-15 10:00:00", "item_id": "item_42"})
    assert df.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
        "item_id": "item_42",
        "ItemNameFeature": "name_42",
        "ItemTypeFeature": "type_42",
    }
