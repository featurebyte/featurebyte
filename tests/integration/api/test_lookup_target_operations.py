"""
Lookup target operations
"""
import pandas as pd
import pytest

from tests.integration.api.feature_preview_utils import (
    convert_preview_param_dict_to_feature_preview_resp,
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
