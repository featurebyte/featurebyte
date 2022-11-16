"""
Unit test for query graph
"""
import os.path
import textwrap
from collections import defaultdict

import pytest
from bson import json_util
from bson.objectid import ObjectId

from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalGraphState, GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.node import construct_node
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
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
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    assert len(graph.nodes) == node_num
    node_another_duplicated = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"], "unknown": "whatever"},
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
        "parameters": {"columns": ["a"]},
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


def test_prune__redundant_assign_nodes(dataframe):
    """
    Test graph pruning on a query graph with redundant assign nodes
    """
    dataframe["redundantA"] = dataframe["CUST_ID"] / 10
    dataframe["redundantB"] = dataframe["VALUE"] + 10
    dataframe["target"] = dataframe["CUST_ID"] * dataframe["VALUE"]
    assert dataframe.node == construct_node(
        name="assign_3", type="assign", parameters={"name": "target"}, output_type="frame"
    )
    pruned_graph, node_name_map = dataframe.graph.prune(
        target_node=dataframe.node, target_columns={"target"}
    )
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    assert pruned_graph.edges_map == {
        "input_1": ["project_1", "project_2", "assign_1"],
        "project_1": ["mul_1"],
        "project_2": ["mul_1"],
        "mul_1": ["assign_1"],
    }
    assert pruned_graph.nodes_map["assign_1"] == {
        "name": "assign_1",
        "type": "assign",
        "parameters": {"name": "target", "value": None},
        "output_type": "frame",
    }
    assert mapped_node.name == "assign_1"


def test_prune__redundant_assign_node_with_same_target_column_name(dataframe):
    """
    Test graph pruning on a query graph with redundant assign node of same target name
    """
    dataframe["VALUE"] = 1
    dataframe["VALUE"] = dataframe["CUST_ID"] * 10
    # convert the dataframe into dictionary & compare some attribute values
    dataframe_dict = dataframe.dict()
    assert dataframe_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "project_1"},
        {"source": "project_1", "target": "mul_1"},
        {"source": "input_1", "target": "assign_1"},
        {"source": "mul_1", "target": "assign_1"},
    ]
    pruned_graph, node_name_map = dataframe.graph.prune(
        target_node=dataframe.node, target_columns={"VALUE"}
    )
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    assert pruned_graph.nodes_map["assign_1"].parameters.dict() == {"name": "VALUE", "value": None}
    assert mapped_node.name == "assign_1"


def test_prune__redundant_project_nodes(dataframe):
    """
    Test graph pruning on a query graph with redundant project nodes
    """
    _ = dataframe["CUST_ID"]
    _ = dataframe["VALUE"]
    mask = dataframe["MASK"]
    pruned_graph, node_name_map = dataframe.graph.prune(target_node=mask.node, target_columns=set())
    mapped_node = pruned_graph.get_node_by_name(node_name_map[mask.node.name])
    assert pruned_graph.edges_map == {"input_1": ["project_1"]}
    assert pruned_graph.nodes_map["project_1"].parameters.columns == ["MASK"]
    assert mapped_node.name == "project_1"


def test_prune__multiple_non_redundant_assign_nodes__interactive_pattern(dataframe):
    """
    Test graph pruning on a query graph without any redundant assign nodes (interactive pattern)
    """
    dataframe["requiredA"] = dataframe["CUST_ID"] / 10
    dataframe["requiredB"] = dataframe["VALUE"] + 10
    dataframe["target"] = dataframe["requiredA"] * dataframe["requiredB"]
    pruned_graph, node_name_map = dataframe.graph.prune(
        target_node=dataframe.node, target_columns={"target"}
    )
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    assert pruned_graph.edges_map == {
        "input_1": ["project_1", "assign_1", "project_3", "assign_2"],
        "project_1": ["add_1"],
        "project_2": ["mul_1"],
        "project_3": ["div_1"],
        "project_4": ["mul_1"],
        "add_1": ["assign_1"],
        "assign_1": ["project_2", "assign_3"],
        "assign_2": ["project_4"],
        "div_1": ["assign_2"],
        "mul_1": ["assign_3"],
    }
    assert pruned_graph.nodes_map["assign_1"].parameters.name == "requiredB"
    assert pruned_graph.nodes_map["assign_2"].parameters.name == "requiredA"
    assert pruned_graph.nodes_map["project_1"].parameters.columns == ["VALUE"]
    assert pruned_graph.nodes_map["project_2"].parameters.columns == ["requiredB"]
    assert pruned_graph.nodes_map["project_3"].parameters.columns == ["CUST_ID"]
    assert pruned_graph.nodes_map["project_4"].parameters.columns == ["requiredA"]
    assert mapped_node.name == "assign_3"


def test_prune__multiple_non_redundant_assign_nodes__cascading_pattern(dataframe):
    """
    Test graph pruning on a query graph without any redundant assign nodes (cascading pattern)
    """
    dataframe["requiredA"] = dataframe["CUST_ID"] / 10
    dataframe["requiredB"] = dataframe["requiredA"] + 10
    dataframe["target"] = dataframe["requiredB"] * 10
    pruned_graph, node_name_map = dataframe.graph.prune(
        target_node=dataframe.node, target_columns={"target"}
    )
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    assert pruned_graph.edges_map == {
        "input_1": ["project_1", "assign_1"],
        "project_1": ["div_1"],
        "div_1": ["assign_1"],
        "assign_1": ["project_2", "assign_2"],
        "project_2": ["add_1"],
        "add_1": ["assign_2"],
        "assign_2": ["project_3", "assign_3"],
        "project_3": ["mul_1"],
        "mul_1": ["assign_3"],
    }
    assert pruned_graph.nodes_map["assign_1"].parameters.name == "requiredA"
    assert pruned_graph.nodes_map["assign_2"].parameters.name == "requiredB"
    assert pruned_graph.nodes_map["project_1"].parameters.columns == ["CUST_ID"]
    assert pruned_graph.nodes_map["project_2"].parameters.columns == ["requiredA"]
    assert pruned_graph.nodes_map["project_3"].parameters.columns == ["requiredB"]
    assert mapped_node.name == "assign_3"


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
    pruned_graph, node_name_map = dataframe.graph.prune(
        target_node=dataframe.node, target_columns=set(dataframe.columns)
    )
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
    }

    # construct the query of the last node
    node_before_load, columns_before_load = dataframe.node, dataframe.columns
    pruned_graph_before_load, node_name_map_before_load = dataframe.graph.prune(
        target_node=node_before_load, target_columns=set(columns_before_load)
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
        target_node=node_before_load, target_columns=set(columns_before_load)
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
    output = query_graph_with_groupby.reconstruct(replace_nodes_map={})
    assert output.dict()["edges"] == query_graph_with_groupby.dict()["edges"]
    assert output.nodes_map["groupby_1"] == query_graph_with_groupby.nodes_map["groupby_1"]


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
    output = query_graph_with_groupby.reconstruct(replace_nodes_map=replace_nodes_map)

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
    groupby_node = graph.add_groupby_operation(
        node_params=groupby_node_params, input_node=node_input
    )
    tile_id = "transaction_f3600_m1800_b900_8a2a4064239908696910f175aa0f4b69105997f3"
    aggregation_id = "sum_925a5866dd2cbfe915e070831311f860176d09c7"
    assert groupby_node.parameters.tile_id == tile_id
    assert groupby_node.parameters.aggregation_id == aggregation_id


def test_query_graph__add_groupby_operation__error(groupby_node_params):
    """Test add_groupby_operation method (value error)"""
    query_graph = QueryGraph()
    input_node = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["random"]},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    with pytest.raises(ValueError) as exc:
        query_graph.add_groupby_operation(
            node_params=groupby_node_params,
            input_node=input_node,
        )
    assert "Failed to add groupby operation." in str(exc)


def test_query_graph__representation():
    """Test the graph can be represented properly without throwing exceptions"""
    graph = QueryGraph()
    graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "event_data",
            "id": ObjectId("633844bd416657bb96c96d3f"),
            "columns": ["column"],
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
                            "column"
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
                        "type": "event_data",
                        "timestamp": null,
                        "id": "633844bd416657bb96c96d3f"
                    }
                }
            ]
        }
        """
    ).strip()
    assert repr(graph) == expected
    assert str(graph) == expected


def test_prune__item_view_join_event_view(test_dir):
    """Test graph pruning on item view join with event view"""
    fixture_path = os.path.join(test_dir, "fixtures/graph/event_item_view_join.json")
    with open(fixture_path) as fhandle:
        graph_dict = json_util.loads(fhandle.read())

    query_graph = QueryGraph(**graph_dict)
    assert "assign_1" in query_graph.nodes_map

    # check that assign node not get pruned
    target_node = query_graph.get_node_by_name("join_2")
    pruned_graph, _ = query_graph.prune(
        target_node=target_node, target_columns=set(target_node.get_new_output_columns())
    )
    assert "assign_1" in pruned_graph.nodes_map
