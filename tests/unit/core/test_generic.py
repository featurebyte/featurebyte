"""
Tests QueryObject
"""
import pytest

from featurebyte.core.generic import QueryObject
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph


def check_graph_state(graph1, graph2):
    """
    Check the reference id of the graph1 & graph2
    """
    assert id(graph1.edges) == id(graph2.edges)
    assert id(graph1.backward_edges) == id(graph2.backward_edges)
    assert id(graph1.nodes) == id(graph2.nodes)
    assert id(graph1.node_type_counter) == id(graph2.node_type_counter)
    assert id(graph1.ref_to_node_name) == id(graph2.ref_to_node_name)


@pytest.fixture(name="tabular_source")
def tabular_source_fixture(snowflake_feature_store):
    """
    Tabulor source fixture
    """
    return (
        snowflake_feature_store,
        {"database_name": "db", "schema_name": "public", "table_name": "some_table_name"},
    )


@pytest.fixture(name="query_object1")
def query_object1_fixture(snowflake_feature_store, tabular_source):
    """
    Query Object 1 fixture
    """
    global_graph = GlobalQueryGraph()
    node_proj1 = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[],
    )
    query_obj1 = QueryObject(
        node=node_proj1,
        row_index_lineage=(node_proj1.name,),
        tabular_source=tabular_source,
    )
    check_graph_state(global_graph, query_obj1.graph)
    return query_obj1


@pytest.fixture(name="query_object2")
def query_object2_fixture(query_object1, tabular_source):
    """
    Query Object 2 fixture
    """
    global_graph = GlobalQueryGraph()
    node_proj1 = query_object1.node
    node_proj2 = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"other": "column"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_proj1],
    )
    query_obj2 = QueryObject(
        node=node_proj2,
        row_index_lineage=(node_proj1.name, node_proj2.name),
        tabular_source=tabular_source,
    )
    check_graph_state(global_graph, query_object1.graph)
    check_graph_state(query_object1.graph, query_obj2.graph)
    return query_obj2
