"""
Lookup feature operations
"""
from featurebyte import ItemView
from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
)


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
    new_feature_preview = new_feature.preview(preview_params)
    assert new_feature_preview.iloc[0].to_dict() == {
        new_feature.name: False,
        **convert_preview_param_dict_to_feature_preview_resp(preview_params),
    }
