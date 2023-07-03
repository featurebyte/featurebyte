"""
Test sanitizer module
"""
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.service.sanitizer import sanitize_query_graph_for_feature_definition


def test_sanitize_query_graph_for_feature_creation(float_feature, non_time_based_feature):
    """Test sanitize query graph for feature creation"""
    features = [float_feature, non_time_based_feature]
    for feat in features:
        # sanitize the graph & check post sanitization view mode
        sanitized_graph = sanitize_query_graph_for_feature_definition(graph=feat.graph)
        for node in sanitized_graph.iterate_sorted_graph_nodes(
            graph_node_types=GraphNodeType.view_graph_node_types()
        ):
            assert node.parameters.metadata.view_mode == "manual"
