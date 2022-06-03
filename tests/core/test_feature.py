"""
Unit test for Feature & FeatureList classes
"""
import pytest

from featurebyte.core.feature import Feature, FeatureList
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node


def test_feature_list__getitem_list_of_str(feature_list):
    """
    Test retrieving single column
    """
    feature_list_subset = feature_list[["sum_2h", "sum_1d"]]
    assert isinstance(feature_list_subset, FeatureList)
    assert feature_list_subset.protected_columns == {"cust_id"}
    assert feature_list_subset.entity_identifiers == ["cust_id"]
    assert feature_list_subset.inception_node == feature_list.inception_node
