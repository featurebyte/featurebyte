"""
Tests QueryObject
"""
from __future__ import annotations

import copy

import pytest

from featurebyte import SnowflakeDetails
from featurebyte.api.feature_store import FeatureStore
from featurebyte.core.generic import QueryObject
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, Node, QueryGraph


def check_graph_state(graph1, graph2):
    """
    Check the reference id of the graph1 & graph2
    """
    assert isinstance(graph1, GlobalQueryGraph)
    assert isinstance(graph2, GlobalQueryGraph)
    assert id(graph1.edges) == id(graph2.edges)
    assert id(graph1.nodes) == id(graph2.nodes)
    assert id(graph1.node_type_counter) == id(graph2.node_type_counter)
    assert id(graph1.ref_to_node_name) == id(graph2.ref_to_node_name)
    assert id(graph1.node_name_to_ref) == id(graph2.node_name_to_ref)


@pytest.fixture(name="feature_store_tabular_source")
def feature_store_tabular_source_fixture():
    """
    Tabulor source fixture
    """
    feature_store = FeatureStore(
        name="sf_featurestore",
        type="snowflake",
        details=SnowflakeDetails(
            account="sf_account",
            warehouse="sf_warehouse",
            sf_schema="sf_schema",
            database="sf_database",
        ),
    )
    tabular_source = {
        "feature_store_id": feature_store.id,
        "table_details": {
            "database_name": "db",
            "schema_name": "public",
            "table_name": "some_table_name",
        },
    }
    return feature_store, tabular_source


@pytest.fixture(name="query_object1")
def query_object1_fixture(feature_store_tabular_source):
    """
    Query Object 1 fixture
    """
    global_graph = GlobalQueryGraph()
    feature_store, tabular_source = feature_store_tabular_source
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "source_table",
            "columns": ["column"],
            "table_details": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "random_table",
            },
            "feature_store_details": feature_store.json_dict(),
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    query_obj1 = QueryObject(
        feature_store=feature_store,
        node_name=node_input.name,
        tabular_source=tabular_source,
    )
    check_graph_state(global_graph, query_obj1.graph)
    return query_obj1


@pytest.fixture(name="query_object2")
def query_object2_fixture(feature_store_tabular_source, query_object1):
    """
    Query Object 2 fixture
    """
    global_graph = GlobalQueryGraph()
    node_input = query_object1.node
    node_proj = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["column_name"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    feature_store, tabular_source = feature_store_tabular_source
    query_obj2 = QueryObject(
        feature_store=feature_store,
        node_name=node_proj.name,
        tabular_source=tabular_source,
    )
    check_graph_state(global_graph, query_object1.graph)
    check_graph_state(query_object1.graph, query_obj2.graph)
    return query_obj2


def test_copy_global_query_graph(query_object1, query_object2):
    """
    Test copy on global query graph
    """
    global_graph = GlobalQueryGraph()
    query_object3 = query_object1.copy()
    assert query_object3 == query_object1
    query_object4 = query_object2.copy(deep=True)
    assert query_object4 == query_object2
    query_object5 = copy.copy(query_object3)
    query_object6 = copy.deepcopy(query_object4)
    check_graph_state(query_object1.graph, global_graph)
    check_graph_state(query_object2.graph, global_graph)
    check_graph_state(query_object3.graph, global_graph)
    check_graph_state(query_object4.graph, global_graph)
    check_graph_state(query_object5.graph, global_graph)
    check_graph_state(query_object6.graph, global_graph)
