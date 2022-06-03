"""
Unit test for Feature & FeatureList classes
"""
import pytest

from featurebyte.core.feature import Feature, FeatureList
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node


def test_feature_list__getitem__list_of_str(feature_list):
    """
    Test retrieving single column
    """
    feature_list_subset = feature_list[["sum_2h", "sum_1d"]]
    assert isinstance(feature_list_subset, FeatureList)
    assert feature_list_subset.protected_columns == {"cust_id"}
    assert feature_list_subset.entity_identifiers == ["cust_id"]
    assert feature_list_subset.inception_node == feature_list.inception_node


def test_feature_list__getitem__series_key(feature_list, bool_feature):
    """
    Test filtering on feature list object
    """
    feature_list_subset = feature_list[bool_feature]
    assert isinstance(feature_list_subset, FeatureList)
    assert feature_list_subset.inception_node == feature_list.inception_node


def test_feature_list__override_protected_column(feature_list):
    """
    Test attempting to change feature list's protected column value
    """
    assert "cust_id" in feature_list.protected_columns
    with pytest.raises(ValueError) as exc:
        feature_list["cust_id"] = feature_list["cust_id"] * 2
    expected_msg = "Entity identifier column 'cust_id' cannot be modified!"
    assert expected_msg in str(exc.value)


def test_feature__binary_ops_return_feature_type(float_feature):
    """
    Test Feature return correct type after binary operation
    """
    output = float_feature + float_feature
    assert isinstance(output, Feature)
    assert output.inception_node == float_feature.inception_node
    assert output.protected_columns == float_feature.protected_columns


def test_feature__getitem__series_key(float_feature, bool_feature):
    """
    Test Feature filtering
    """
    output = float_feature[bool_feature]
    assert isinstance(output, Feature)
    assert output.inception_node == float_feature.inception_node
    assert output.protected_columns == float_feature.protected_columns

    with pytest.raises(TypeError) as exc:
        _ = bool_feature[float_feature]
    assert "Only boolean Series filtering is supported!" in str(exc.value)


def test_feature__bool_series_key_scalar_value(float_feature, bool_feature):
    """
    Test Feature conditional assignment
    """
    float_feature[bool_feature] = 10
    assert float_feature.node == Node(
        name="cond_assign_1",
        type=NodeType.COND_ASSIGN,
        parameters={"value": 10},
        output_type=NodeOutputType.SERIES,
    )
