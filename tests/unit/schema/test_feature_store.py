"""Test feature store related schemas"""

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.feature_store import FeatureStorePreview, FeatureStoreSample


@pytest.fixture(name="query_graph_and_nodes")
def query_graph_and_nodes_fixture():
    """Fixture for a QueryGraph"""
    graph = QueryGraph()
    input_details = {
        "table_details": {
            "database_name": "db",
            "schema_name": "public",
            "table_name": "event_table",
        },
        "feature_store_details": {
            "type": "snowflake",
            "details": {
                "database_name": "db",
                "schema_name": "public",
                "role_name": "role",
                "account": "account",
                "warehouse": "warehouse",
            },
        },
    }
    node_params = {
        "type": "event_table",
        "columns": [
            {"name": "ts", "dtype": DBVarType.TIMESTAMP},
            {"name": "cust_id", "dtype": DBVarType.INT},
            {"name": "biz_id", "dtype": DBVarType.INT},
            {"name": "product_type", "dtype": DBVarType.INT},
            {"name": "a", "dtype": DBVarType.FLOAT},
            {"name": "b", "dtype": DBVarType.FLOAT},
        ],
        "timestamp_column": "ts",
        **input_details,
    }
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    proj_b = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    return graph, {"node_input": node_input, "node_a": proj_a, "node_b": proj_b}


def test_feature_store_preview(query_graph_and_nodes):
    """
    Test FeatureStorePreview schema
    """
    graph, nodes = query_graph_and_nodes
    feature_store_preview = FeatureStorePreview(
        graph=graph,
        node_name=nodes["node_a"].name,
        feature_store_id=None,
        enable_query_cache=True,
    )
    assert set(feature_store_preview.graph.nodes_map.keys()) == {"input_1", "project_1"}
    assert feature_store_preview.graph.nodes_map["project_1"].parameters.columns == ["a"]
    assert nodes["node_a"].parameters.columns == ["a"]

    # attempt to update graph in feature store preview, it should not affect the original graph
    feature_store_preview.graph.nodes_map["project_1"].parameters.columns = ["b"]
    assert feature_store_preview.graph.nodes_map["project_1"].parameters.columns == ["b"]
    assert nodes["node_a"].parameters.columns == ["a"]


def test_feature_store_sample(query_graph_and_nodes):
    """
    Test FeatureStoreSample schema
    """
    graph, nodes = query_graph_and_nodes
    feature_store_sample = FeatureStoreSample(
        graph=graph,
        node_name=nodes["node_a"].name,
        feature_store_id=None,
        from_timestamp=None,
        to_timestamp=None,
        timestamp_column="ts",
        stats_names=["mean", "std"],
    )
    assert set(feature_store_sample.graph.nodes_map.keys()) == {"input_1", "project_1"}
    assert feature_store_sample.graph.nodes_map["project_1"].parameters.columns == ["a"]
    assert nodes["node_a"].parameters.columns == ["a"]

    # attempt to update graph in feature store sample, it should not affect the original graph
    feature_store_sample.graph.nodes_map["project_1"].parameters.columns = ["b"]
    assert feature_store_sample.graph.nodes_map["project_1"].parameters.columns == ["b"]
    assert nodes["node_a"].parameters.columns == ["a"]
