"""
Tests QueryObject
"""
from featurebyte.core.generic import QueryObject
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph


def test_query_object(snowflake_feature_store):
    global_graph = GlobalQueryGraph()
    tabular_source = (
        snowflake_feature_store,
        {"database_name": "db", "schema_name": "public", "table_name": "some_table_name"},
    )
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
    assert id(query_obj1.graph.edges) == id(query_obj2.graph.edges) == id(global_graph.edges)
    assert (
        id(query_obj1.graph.backward_edges)
        == id(query_obj2.graph.backward_edges)
        == id(global_graph.backward_edges)
    )
    assert id(query_obj1.graph.nodes) == id(query_obj2.graph.nodes) == id(global_graph.nodes)
    assert (
        id(query_obj1.graph.node_type_counter)
        == id(query_obj2.graph.node_type_counter)
        == id(global_graph.node_type_counter)
    )
    assert (
        id(query_obj1.graph.ref_to_node_name)
        == id(query_obj2.graph.ref_to_node_name)
        == id(global_graph.ref_to_node_name)
    )
