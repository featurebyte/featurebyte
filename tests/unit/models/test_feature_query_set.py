"""
Tests for featurebyte/models/feature_query_set.py
"""

import pytest

from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.feature_historical import HistoricalFeatureQueryGenerator


@pytest.fixture(name="feature_model")
def feature_model_fixture(float_feature):
    """
    Fixture for a saved feature model
    """
    float_feature.save()
    return float_feature.cached_model


@pytest.fixture(name="feature_query_generator")
def feature_query_generator_fixture(feature_model, source_info):
    """
    Fixture for FeatureQueryGenerator
    """
    generator = HistoricalFeatureQueryGenerator(
        graph=feature_model.graph,
        nodes=[feature_model.node],
        request_table_name="my_request_table",
        request_table_columns=["POINT_IN_TIME", "cust_id"],
        source_info=source_info,
        output_table_details=TableDetails(table_name="my_output_table"),
        output_feature_names=[feature_model.name],
    )
    return generator


def test_feature_query_generator(feature_query_generator, feature_model):
    """
    Test FeatureQueryGenerator
    """
    assert feature_query_generator.get_nodes() == [feature_model.node]
    assert feature_query_generator.get_node_names() == [feature_model.node.name]
    assert feature_query_generator.get_feature_names(
        [feature_model.node.name]
    ) == [feature_model.name]
