"""
Tests for featurebyte/query_graph/sql/batch_helper.p
"""

import pytest

from featurebyte.query_graph.sql.batch_helper import split_nodes


@pytest.fixture
def features_set_1(snowflake_event_view_with_entity, feature_group_feature_job_setting):
    """
    Fixture for a set of features that should be grouped together by split_nodes
    """
    view = snowflake_event_view_with_entity
    features = []
    for method in ["sum", "min", "max"]:
        feature_name = "feature_set_1_" + method
        feature = view.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method=method,
            windows=["30m"],
            feature_job_setting=feature_group_feature_job_setting,
            feature_names=[feature_name],
        )[feature_name]
        features.append(feature)
    return features


@pytest.fixture
def features_set_2(snowflake_event_view_with_entity, feature_group_feature_job_setting):
    """
    Fixture for a set of features that should be grouped together by split_nodes (different groupby
    key compared to features_set_1)
    """
    view = snowflake_event_view_with_entity
    features = []
    for method in ["sum", "min", "max"]:
        feature_name = "feature_set_2_" + method
        feature = view.groupby("col_int").aggregate_over(
            value_column="col_float",
            method=method,
            windows=["30m"],
            feature_job_setting=feature_group_feature_job_setting,
            feature_names=[feature_name],
        )[feature_name]
        features.append(feature)
    return features


def test_split_nodes(features_set_1, features_set_2, source_info):
    """
    Test split_nodes
    """
    graph = features_set_1[0].graph
    nodes_set_1 = [feature.node for feature in features_set_1]
    nodes_set_2 = [feature.node for feature in features_set_2]
    interspersed_nodes = [
        nodes_set_1[0],
        nodes_set_2[0],
        nodes_set_1[1],
        nodes_set_2[1],
        nodes_set_1[2],
        nodes_set_2[2],
    ]
    result = split_nodes(
        graph=graph,
        nodes=interspersed_nodes,
        num_features_per_query=3,
        source_info=source_info,
    )

    def _get_node_names(nodes):
        return tuple(sorted([node.name for node in nodes]))

    # Set up expected node names after split. Main thing to check is that the nodes are grouped
    # together; the order of the groups doesn't matter.
    expected_node_names_set_1 = _get_node_names(nodes_set_1)
    expected_node_names_set_2 = _get_node_names(nodes_set_2)
    expected_node_names = sorted([expected_node_names_set_1, expected_node_names_set_2])

    # Set up actual node names after split
    actual_node_names_set_1 = _get_node_names(result[0])
    actual_node_names_set_2 = _get_node_names(result[1])
    actual_node_names = sorted([actual_node_names_set_1, actual_node_names_set_2])

    assert actual_node_names == expected_node_names


def test_split_nodes__forecast_point_dt_feature(
    days_until_forecast_feature,
    forecast_point_dt_feature,
    source_info,
):
    """
    Test split_nodes can handle forecast point datetime features
    """
    graph = days_until_forecast_feature.graph
    nodes = [
        days_until_forecast_feature.node,
        forecast_point_dt_feature.node,
    ]
    result = split_nodes(
        graph=graph,
        nodes=nodes,
        num_features_per_query=1,
        source_info=source_info,
    )
    assert len(result) == 2
    assert result[0][0].parameters.name == "days_until_forecast"
    assert result[1][0].parameters.name == "forecast_point_day"
