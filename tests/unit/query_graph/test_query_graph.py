"""
Unit test for query graph
"""
import textwrap
from collections import defaultdict

import pytest
from bson.objectid import ObjectId

from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalGraphState, GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.node import construct_node
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.transform.reconstruction import (
    GroupByNode,
    add_pruning_sensitive_operation,
)
from tests.util.helper import get_node


def check_internal_state_after_deserialization(graph):
    """Check internal state after deserialization"""
    # extract internal variables
    internal_fields = [
        "node_type_counter",
        "node_name_to_ref",
        "ref_to_node_name",
        "nodes_map",
        "edges_map",
        "backward_edges_map",
    ]
    internal_vars = {}
    for field in internal_fields:
        internal_vars[field] = getattr(graph, field).copy()

    # convert global graph to query graph first
    query_graph = QueryGraph(**graph.dict())

    # check whether the internal variables are set properly
    for field in internal_fields:
        assert getattr(query_graph, field) == internal_vars[field]


def test_add_operation__add_duplicated_node_on_two_nodes_graph(graph_two_nodes):
    """
    Test add operation by adding a duplicated node on a 2-node graph
    """
    graph, node_input, node_proj = graph_two_nodes
    check_internal_state_after_deserialization(graph)

    # check no new nodes are added
    node_num = len(graph.nodes)
    node_duplicated = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["column"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    assert len(graph.nodes) == node_num
    node_another_duplicated = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["column"], "unknown": "whatever"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    assert node_duplicated == node_another_duplicated
    assert len(graph.nodes) == node_num

    graph_dict = graph.dict()
    input_node = get_node(graph_dict, "input_1")
    project_node = get_node(graph_dict, "project_1")
    assert input_node == {
        "name": "input_1",
        "type": "input",
        "parameters": input_node["parameters"],
        "output_type": "frame",
    }
    assert project_node == {
        "name": "project_1",
        "type": "project",
        "parameters": {"columns": ["column"]},
        "output_type": "series",
    }
    assert graph_dict["edges"] == [{"source": "input_1", "target": "project_1"}]
    assert node_duplicated == node_proj


def test_add_operation__add_duplicated_node_on_four_nodes_graph(graph_four_nodes):
    """
    Test add operation by adding a duplicated node on a 4-node graph
    """
    graph, _, node_proj, node_eq, _ = graph_four_nodes
    check_internal_state_after_deserialization(graph)
    node_duplicated = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_proj],
    )
    assert node_duplicated == node_eq


def test_serialization_deserialization__clean_global_graph(graph_four_nodes):
    """
    Test serialization & deserialization of query graph object (clean global query graph)
    """
    graph, _, _, _, _ = graph_four_nodes
    check_internal_state_after_deserialization(graph)
    graph_dict = graph.dict()
    deserialized_graph = QueryGraph.parse_obj(graph_dict)
    assert graph == deserialized_graph

    # clean up global query graph state & load the deserialized graph to the clean global query graph
    GlobalGraphState.reset()
    new_global_graph = GlobalQueryGraph()
    assert new_global_graph.nodes == []
    new_global_graph.load(graph)
    assert new_global_graph.dict() == graph_dict


def test_serialization_deserialization__with_existing_non_empty_graph(dataframe):
    """
    Test serialization & deserialization of query graph object (non-empty global query graph)
    """
    # pylint: disable=too-many-locals
    # construct a graph
    dataframe["feature"] = dataframe["VALUE"] * dataframe["CUST_ID"] / 100.0
    dataframe = dataframe[dataframe["MASK"]]

    # serialize the graph with the last node of the graph
    node_names = set(dataframe.graph.nodes_map)
    pruned_graph, node_name_map = dataframe.graph.prune(target_node=dataframe.node, aggressive=True)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    query_before_serialization = GraphInterpreter(
        pruned_graph, SourceType.SNOWFLAKE
    ).construct_preview_sql(node_name=mapped_node.name)

    # further modify the global graph & check the global query graph are updated
    dataframe["feature"] = dataframe["VALUE"] / dataframe["CUST_ID"]
    dataframe["CUST_ID"] = dataframe["CUST_ID"] + 10
    assert set(dataframe.graph.nodes_map).difference(node_names) == {
        "add_1",
        "assign_2",
        "assign_3",
        "div_2",
        "project_4",
        "project_5",
        "project_6",
    }

    # construct the query of the last node
    node_before_load, columns_before_load = dataframe.node, dataframe.columns
    pruned_graph_before_load, node_name_map_before_load = dataframe.graph.prune(
        target_node=node_before_load, aggressive=True
    )
    mapped_node_before_load = pruned_graph_before_load.get_node_by_name(
        node_name_map_before_load[node_before_load.name]
    )
    query_before_load = GraphInterpreter(
        pruned_graph_before_load, SourceType.SNOWFLAKE
    ).construct_preview_sql(mapped_node_before_load.name)

    # deserialize the graph, load the graph to global query graph & check the generated query
    graph = QueryGraph.parse_obj(pruned_graph.dict())
    _, node_name_map = GlobalQueryGraph().load(graph)
    node_global = GlobalQueryGraph().get_node_by_name(node_name_map[mapped_node.name])
    assert (
        GraphInterpreter(GlobalQueryGraph(), SourceType.SNOWFLAKE).construct_preview_sql(
            node_global.name
        )
        == query_before_serialization
    )
    assert isinstance(graph.edges_map, defaultdict)
    assert isinstance(graph.backward_edges_map, defaultdict)
    assert isinstance(graph.node_type_counter, defaultdict)

    # check that loading the deserialized graph back to global won't affect other node
    pruned_graph_after_load, _ = GlobalQueryGraph().prune(
        target_node=node_before_load, aggressive=True
    )
    query_after_load = GraphInterpreter(
        pruned_graph_after_load, SourceType.SNOWFLAKE
    ).construct_preview_sql(mapped_node_before_load.name)
    assert query_before_load == query_after_load


def test_global_graph_attributes():
    """Test global graph attributes shared across different global graph instances"""
    graph1 = GlobalQueryGraph()
    graph2 = GlobalQueryGraph()
    assert id(graph1.nodes) == id(graph2.nodes)
    assert id(graph1.edges) == id(graph2.edges)
    assert id(graph1.nodes_map) == id(graph2.nodes_map)
    assert id(graph1.edges_map) == id(graph2.edges_map)
    assert id(graph1.backward_edges_map) == id(graph2.backward_edges_map)
    assert id(graph1.node_type_counter) == id(graph2.node_type_counter)
    assert id(graph1.node_name_to_ref) == id(graph2.node_name_to_ref)


def test_query_graph__reconstruct_edge_case(query_graph_with_groupby):
    """Test reconstruct class method (edge case)"""
    output, _ = query_graph_with_groupby.reconstruct(
        node_name_to_replacement_node={}, regenerate_groupby_hash=False
    )
    expected_tile_id = "TILE_F3600_M1800_B900_7BD30FF1B8E84ADD2B289714C473F1A21E9BC624"
    assert output.edges_map == query_graph_with_groupby.edges_map
    assert output.nodes_map["groupby_1"] == query_graph_with_groupby.nodes_map["groupby_1"]
    assert output.nodes_map["groupby_1"].parameters.tile_id == expected_tile_id

    # check that tile id is different if regenerate_groupby_hash=True
    expected_tile_id = "TILE_F3600_M1800_B900_44F5A6D9500FD740581C63DBB87A6770DE6AB633"
    output, _ = query_graph_with_groupby.reconstruct(
        node_name_to_replacement_node={}, regenerate_groupby_hash=True
    )
    assert output.edges_map == query_graph_with_groupby.edges_map
    assert output.nodes_map["groupby_1"] != query_graph_with_groupby.nodes_map["groupby_1"]
    assert output.nodes_map["groupby_1"].parameters.tile_id == expected_tile_id


@pytest.mark.parametrize(
    "replacement_map",
    [
        {"groupby_1": {"blind_spot": 300}},
        {"assign_1": {"name": "hello"}},
        {"input_1": {"columns": ["hello", "world"]}},
    ],
)
def test_query_graph__reconstruct(query_graph_with_groupby, replacement_map):
    """Test reconstruct class method"""
    replace_nodes_map = {}
    for node_name, other_params in replacement_map.items():
        node = query_graph_with_groupby.get_node_by_name(node_name)
        parameters = {**node.parameters.dict(), **other_params}
        replace_node = construct_node(**{**node.dict(), "parameters": parameters})
        assert replace_node != node
        replace_nodes_map[node_name] = replace_node

    assert len(replacement_map) > 0
    output, _ = query_graph_with_groupby.reconstruct(
        node_name_to_replacement_node=replace_nodes_map, regenerate_groupby_hash=False
    )

    # check replace_node found in the output query graph
    for node_name, replace_node in replace_nodes_map.items():
        found = False
        assert query_graph_with_groupby.nodes_map[node_name] != replace_node
        for node in output.nodes:
            if node.dict(exclude={"name": True}) == replace_node.dict(exclude={"name": True}):
                found = True
        assert found


@pytest.fixture(name="groupby_node_params")
def groupby_node_params_fixture():
    """Groupby Node parameters"""
    return {
        "keys": ["biz_id"],
        "value_by": None,
        "parent": "a",
        "agg_func": "sum",
        "time_modulo_frequency": 1800,
        "frequency": 3600,
        "blind_spot": 900,
        "timestamp": "ts",
        "names": ["a_7d_sum_by_business"],
        "windows": ["7d"],
        "serving_names": ["BUSINESS_ID"],
    }


def test_query_graph__add_groupby_operation(graph_single_node, groupby_node_params):
    """Test add_groupby_operation method"""
    graph, node_input = graph_single_node
    assert "tile_id" not in groupby_node_params
    assert "aggregation_id" not in groupby_node_params
    groupby_node = add_pruning_sensitive_operation(
        graph=graph, node_cls=GroupByNode, node_params=groupby_node_params, input_node=node_input
    )
    tile_id = "TILE_F3600_M1800_B900_89C17DAA4D06AB13E3BFCAEB0AF236CE0CCA713F"
    aggregation_id = "sum_35a40dbf3534a4c4fc204b5101187cf81556efc4"
    assert groupby_node.parameters.tile_id == tile_id
    assert groupby_node.parameters.aggregation_id == aggregation_id


def test_query_graph__add_groupby_operation_with_graph_node(
    query_graph_with_cleaning_ops_graph_node, groupby_node_params
):
    """Test add groupby operation with graph node"""
    graph, graph_node = query_graph_with_cleaning_ops_graph_node
    assert "tile_id" not in groupby_node_params
    assert "aggregation_id" not in groupby_node_params
    groupby_node = add_pruning_sensitive_operation(
        graph=graph, node_cls=GroupByNode, node_params=groupby_node_params, input_node=graph_node
    )
    tile_id = "TILE_F3600_M1800_B900_44F5A6D9500FD740581C63DBB87A6770DE6AB633"
    aggregation_id = "sum_8a508c0abe8012b3650ed6e7e125b01ff70a8930"
    assert groupby_node.parameters.tile_id == tile_id
    assert groupby_node.parameters.aggregation_id == aggregation_id


def test_query_graph__representation():
    """Test the graph can be represented properly without throwing exceptions"""
    graph = QueryGraph()
    graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "event_table",
            "id": ObjectId("633844bd416657bb96c96d3f"),
            "columns": [{"name": "column", "dtype": "INT"}],
            "table_details": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "transaction",
            },
            "feature_store_details": {
                "type": "snowflake",
                "details": {
                    "account": "sf_account",
                    "warehouse": "sf_warehouse",
                    "database": "db",
                    "sf_schema": "public",
                },
            },
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    expected = textwrap.dedent(
        """
        {
            "edges": [],
            "nodes": [
                {
                    "name": "input_1",
                    "type": "input",
                    "output_type": "frame",
                    "parameters": {
                        "columns": [
                            {
                                "name": "column",
                                "dtype": "INT"
                            }
                        ],
                        "table_details": {
                            "database_name": "db",
                            "schema_name": "public",
                            "table_name": "transaction"
                        },
                        "feature_store_details": {
                            "type": "snowflake",
                            "details": {
                                "account": "sf_account",
                                "warehouse": "sf_warehouse",
                                "database": "db",
                                "sf_schema": "public"
                            }
                        },
                        "type": "event_table",
                        "id": "633844bd416657bb96c96d3f",
                        "timestamp_column": null,
                        "id_column": null,
                        "event_timestamp_timezone_offset": null,
                        "event_timestamp_timezone_offset_column": null
                    }
                }
            ]
        }
        """
    ).strip()
    assert repr(graph) == expected
    assert str(graph) == expected


def insert_input_node(graph, input_node_params):
    """Insert input node to the graph"""
    return graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=input_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )


def insert_project_node(graph, input_node, column_name):
    """Insert project node to the graph"""
    return graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": [column_name]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )


def insert_add_node(graph, first_node, second_node):
    """Insert add node to the graph"""
    return graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[first_node, second_node],
    )


@pytest.fixture(name="query_graph_abc_and_node")
def query_graph_abc_and_node_fixture(input_node):
    """Query graph with three project nodes"""
    graph = QueryGraph()
    node_input = insert_input_node(graph, input_node.parameters.dict())
    node_a = insert_project_node(graph, node_input, "a")
    node_b = insert_project_node(graph, node_input, "b")
    node_c = insert_project_node(graph, node_input, "ts")
    node_add_ab = insert_add_node(graph, node_a, node_b)
    node_add_abc = insert_add_node(graph, node_add_ab, node_c)
    return graph, node_add_abc


@pytest.fixture(name="query_graph_cab_and_node")
def query_graph_cab_and_node_fixture(input_node):
    """Query graph with three project nodes"""
    graph = QueryGraph()
    node_input = insert_input_node(graph, input_node.parameters.dict())
    node_c = insert_project_node(graph, node_input, "ts")
    node_a = insert_project_node(graph, node_input, "a")
    node_b = insert_project_node(graph, node_input, "b")
    node_add_ab = insert_add_node(graph, node_a, node_b)
    node_add_abc = insert_add_node(graph, node_add_ab, node_c)
    return graph, node_add_abc


@pytest.fixture(name="query_graph_bca_and_node")
def query_graph_bca_and_node_fixture(input_node):
    """Query graph with three project nodes"""
    graph = QueryGraph()
    node_input = insert_input_node(graph, input_node.parameters.dict())
    node_b = insert_project_node(graph, node_input, "b")
    node_c = insert_project_node(graph, node_input, "ts")
    node_a = insert_project_node(graph, node_input, "a")
    node_add_ab = insert_add_node(graph, node_a, node_b)
    node_add_abc = insert_add_node(graph, node_add_ab, node_c)
    return graph, node_add_abc


def test_query_graph_insensitive_to_node_name(
    query_graph_abc_and_node, query_graph_cab_and_node, query_graph_bca_and_node
):
    """Check that graph ordering is insensitive to node names"""
    query_graph_abc, node_abc = query_graph_abc_and_node
    query_graph_cab, node_cab = query_graph_cab_and_node
    query_graph_bca, node_bca = query_graph_bca_and_node
    assert query_graph_abc != query_graph_cab
    assert query_graph_cab != query_graph_bca

    pruned_graph_abc, node_name_map_abc = query_graph_abc.prune(
        target_node=node_abc, aggressive=False
    )
    pruned_graph_cab, node_name_map_cab = query_graph_cab.prune(
        target_node=node_cab, aggressive=False
    )
    pruned_graph_bca, node_name_map_bca = query_graph_bca.prune(
        target_node=node_bca, aggressive=False
    )
    assert pruned_graph_abc == pruned_graph_cab == pruned_graph_bca
    assert (
        node_name_map_abc[node_abc.name]
        == node_name_map_cab[node_cab.name]
        == node_name_map_bca[node_bca.name]
    )


@pytest.fixture(name="invalid_query_graph_groupby_node")
def invalid_query_graph_groupby_node_fixture(
    snowflake_feature_store_details_dict, snowflake_table_details_dict
):
    """Invalid query graph fixture"""
    groupby_node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "a",
        "agg_func": "avg",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["a_2h_average", "a_48h_average"],
        "windows": ["2h", "48h"],
    }
    graph = QueryGraph()
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "generic",
            "columns": ["random_column"],
            "table_details": snowflake_table_details_dict,
            "feature_store_details": snowflake_feature_store_details_dict,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    node_group_by = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params=groupby_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input],
    )
    return graph, node_group_by
