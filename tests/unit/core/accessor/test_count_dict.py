"""
Unit tests for core/accessor/count_dict.py
"""
import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


@pytest.mark.parametrize(
    "method, method_kwargs, expected_var_type, expected_parameters",
    [
        ("entropy", {}, DBVarType.FLOAT, {"transform_type": "entropy"}),
        ("most_frequent", {}, DBVarType.VARCHAR, {"transform_type": "most_frequent"}),
        (
            "unique_count",
            {},
            DBVarType.FLOAT,
            {"transform_type": "unique_count", "include_missing": True},
        ),
        (
            "unique_count",
            {"include_missing": False},
            DBVarType.FLOAT,
            {"transform_type": "unique_count", "include_missing": False},
        ),
    ],
)
def test_transformation(
    count_per_category_feature, method, method_kwargs, expected_var_type, expected_parameters
):
    new_feature = getattr(count_per_category_feature.cd, method)(**method_kwargs)
    assert new_feature.node.output_type == NodeOutputType.SERIES
    assert new_feature.var_type == expected_var_type
    assert new_feature.node.type == NodeType.COUNT_DICT_TRANSFORM
    assert new_feature.node.parameters == expected_parameters


def test_non_supported_feature_type(bool_feature):
    """Test count dict accessor on non-supported type"""
    with pytest.raises(AttributeError) as exc:
        bool_feature.cd.entropy()
    assert str(exc.value) == "Can only use .cd accessor with count per category features"
