"""
Tests for featurebyte/models/feature_query_set.py
"""


def test_feature_query_generator(feature_query_generator, saved_feature_model):
    """
    Test FeatureQueryGenerator
    """
    feature_model = saved_feature_model
    assert feature_query_generator.get_nodes() == [feature_model.node]
    assert feature_query_generator.get_node_names() == [feature_model.node.name]
    assert feature_query_generator.get_feature_names([feature_model.node.name]) == [
        feature_model.name
    ]
