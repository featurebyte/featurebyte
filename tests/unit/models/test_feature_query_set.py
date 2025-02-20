"""
Tests for featurebyte/models/feature_query_set.py
"""


def test_feature_query_generator(feature_query_generator, saved_features_set):
    """
    Test FeatureQueryGenerator
    """
    graph, nodes, feature_names = saved_features_set
    node_names = [node.name for node in nodes]
    assert feature_query_generator.get_nodes() == nodes
    assert feature_query_generator.get_node_names() == node_names
    assert feature_query_generator.get_feature_names(node_names) == feature_names
