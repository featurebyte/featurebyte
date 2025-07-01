"""
Unit test for query graph
"""

import textwrap

import pytest
from bson import ObjectId

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.node import construct_node
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.transform.reconstruction import (
    GroupByNode,
    add_pruning_sensitive_operation,
)
from tests.util.helper import get_node, reset_global_graph


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
    query_graph = QueryGraph(**graph.model_dump())

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

    graph_dict = graph.model_dump()
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
    graph_dict = graph.model_dump()
    deserialized_graph = QueryGraph.model_validate(graph_dict)
    assert graph == deserialized_graph

    # clean up global query graph state & load the deserialized graph to the clean global query graph
    reset_global_graph()
    new_global_graph = GlobalQueryGraph()
    assert new_global_graph.nodes == []
    new_global_graph.load(graph)
    assert new_global_graph.model_dump() == graph_dict


def test_serialization_deserialization__with_existing_non_empty_graph(dataframe, source_info):
    """
    Test serialization & deserialization of query graph object (non-empty global query graph)
    """

    # construct a graph
    dataframe["feature"] = dataframe["VALUE"] * dataframe["CUST_ID"] / 100.0
    dataframe = dataframe[dataframe["MASK"]]

    # serialize the graph with the last node of the graph
    node_names = set(dataframe.graph.nodes_map)
    pruned_graph, node_name_map = dataframe.graph.prune(target_node=dataframe.node)
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    query_before_serialization = GraphInterpreter(pruned_graph, source_info).construct_preview_sql(
        node_name=mapped_node.name
    )

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
    node_before_load, columns_before_load = dataframe.node, dataframe.columns  # noqa: F841
    pruned_graph_before_load, node_name_map_before_load = dataframe.graph.prune(
        target_node=node_before_load
    )
    mapped_node_before_load = pruned_graph_before_load.get_node_by_name(
        node_name_map_before_load[node_before_load.name]
    )
    query_before_load = GraphInterpreter(
        pruned_graph_before_load, source_info
    ).construct_preview_sql(mapped_node_before_load.name)

    # deserialize the graph, load the graph to global query graph & check the generated query
    graph = QueryGraph.model_validate(pruned_graph.model_dump())
    _, node_name_map = GlobalQueryGraph().load(graph)
    node_global = GlobalQueryGraph().get_node_by_name(node_name_map[mapped_node.name])
    assert (
        GraphInterpreter(GlobalQueryGraph(), source_info).construct_preview_sql(node_global.name)
        == query_before_serialization
    )
    assert isinstance(graph.edges_map, dict)
    assert isinstance(graph.backward_edges_map, dict)
    assert isinstance(graph.node_type_counter, dict)

    # check that loading the deserialized graph back to global won't affect other node
    pruned_graph_after_load, _ = GlobalQueryGraph().prune(target_node=node_before_load)
    query_after_load = GraphInterpreter(pruned_graph_after_load, source_info).construct_preview_sql(
        mapped_node_before_load.name
    )
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
    expected_tile_id = "TILE_F3600_M1800_B900_CD6DA0B88D114A32DB36F42008D3E74E28B5E8FC"
    output, _ = query_graph_with_groupby.reconstruct(
        node_name_to_replacement_node={}, regenerate_groupby_hash=True
    )
    assert output.edges_map == query_graph_with_groupby.edges_map
    assert output.nodes_map["groupby_1"] != query_graph_with_groupby.nodes_map["groupby_1"]
    assert output.nodes_map["groupby_1"].parameters.tile_id == expected_tile_id


@pytest.mark.parametrize(
    "replacement_map",
    [
        {
            "groupby_1": {
                "feature_job_setting": {"blind_spot": "300s", "offset": "1800s", "period": "3600s"}
            }
        },
        {"assign_1": {"name": "hello"}},
        {"input_1": {"columns": ["hello", "world"]}},
    ],
)
def test_query_graph__reconstruct(query_graph_with_groupby, replacement_map):
    """Test reconstruct class method"""
    replace_nodes_map = {}
    for node_name, other_params in replacement_map.items():
        node = query_graph_with_groupby.get_node_by_name(node_name)
        parameters = {**node.parameters.model_dump(), **other_params}
        replace_node = construct_node(**{**node.model_dump(), "parameters": parameters})
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
            if node.model_dump(exclude={"name": True}) == replace_node.model_dump(
                exclude={"name": True}
            ):
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
        "feature_job_setting": {
            "offset": "1800s",
            "period": "3600s",
            "blind_spot": "900s",
        },
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
    tile_id = "TILE_F3600_M1800_B900_4CB5D54DBBC13045F93ADD1FC4B877AA72B22953"
    aggregation_id = "sum_2e6611ffcc5cca12bec34685afb8f08be1748709"
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
    tile_id = "TILE_F3600_M1800_B900_CD6DA0B88D114A32DB36F42008D3E74E28B5E8FC"
    aggregation_id = "sum_d825b8de3ab0320eb0cae221ed59e7c648fcb2e9"
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
                    "database_name": "db",
                    "schema_name": "public",
                    "role_name": "TESTING",
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
                                "dtype": "INT",
                                "dtype_metadata": null,
                                "partition_metadata": null
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
                                "database_name": "db",
                                "schema_name": "public",
                                "role_name": "TESTING"
                            }
                        },
                        "type": "event_table",
                        "id": "633844bd416657bb96c96d3f",
                        "timestamp_column": null,
                        "id_column": null,
                        "event_timestamp_timezone_offset": null,
                        "event_timestamp_timezone_offset_column": null,
                        "event_timestamp_schema": null
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
    node_input = insert_input_node(graph, input_node.parameters.model_dump())
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
    node_input = insert_input_node(graph, input_node.parameters.model_dump())
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
    node_input = insert_input_node(graph, input_node.parameters.model_dump())
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

    pruned_graph_abc, node_name_map_abc = query_graph_abc.quick_prune(
        target_node_names=[node_abc.name]
    )
    pruned_graph_cab, node_name_map_cab = query_graph_cab.quick_prune(
        target_node_names=[node_cab.name]
    )
    pruned_graph_bca, node_name_map_bca = query_graph_bca.quick_prune(
        target_node_names=[node_bca.name]
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
        "feature_job_setting": {
            "offset": "1800s",  # 30m
            "period": "3600s",  # 1h
            "blind_spot": "900s",  # 15m
        },
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


def test_get_table_ids(
    event_table_details, item_table_input_details, snowflake_feature_store_details_dict
):
    """Test get_table_ids method (check that duplicated table ids are not returned)"""
    query_graph = QueryGraph()
    table_id = ObjectId()
    node_input1 = query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "event_table",
            "columns": [{"name": "column", "dtype": "FLOAT"}],
            "table_details": event_table_details.model_dump(),
            "feature_store_details": snowflake_feature_store_details_dict,
            "id": table_id,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    node_input2 = query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "item_table",
            "columns": [{"name": "column", "dtype": "FLOAT"}],
            **item_table_input_details,
            "id": table_id,
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_1 = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["column"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input1],
    )
    proj_2 = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["column"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input2],
    )
    add_node = query_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_1, proj_2],
    )
    table_ids = query_graph.get_table_ids(node_name=add_node.name)
    assert table_ids == [table_id]


def test_input_node_hash_calculation_ignores_feature_store_details(
    event_table_details, snowflake_feature_store_details_dict
):
    """Test that input node hash calculation ignores feature store details"""
    graph = QueryGraph()
    input_node_params = {
        "type": "event_table",
        "columns": [{"name": "column", "dtype": "FLOAT"}],
        "table_details": event_table_details.model_dump(),
        "feature_store_details": snowflake_feature_store_details_dict,
        "id": ObjectId(),
    }
    input_node = insert_input_node(graph, input_node_params)
    assert snowflake_feature_store_details_dict["details"] is not None

    # insert another input node with different feature store details
    input_node_params["feature_store_details"] = {
        "type": snowflake_feature_store_details_dict["type"],
        "details": None,
    }
    second_input_node = insert_input_node(graph, input_node_params)
    assert input_node == second_input_node


def test_global_query_graph__clear(input_node):
    """Test global query graph clear method"""
    graph = GlobalQueryGraph()
    node_input = insert_input_node(graph, input_node.parameters.model_dump())
    insert_project_node(graph, node_input, "a")

    # check serialized attributes
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1

    # check non-serialized attributes
    assert len(graph.nodes_map) == 2
    assert len(graph.edges_map) == 1
    assert len(graph.backward_edges_map) == 1
    assert len(graph.node_type_counter) == 2
    assert len(graph.node_name_to_ref) == 2
    assert len(graph.ref_to_node_name) == 2

    # clear the global query graph
    graph.clear()
    global_graph = GlobalQueryGraph()

    # check serialized attributes
    assert len(global_graph.nodes) == 0
    assert len(global_graph.edges) == 0

    # check non-serialized attributes
    assert len(global_graph.nodes_map) == 0
    assert len(global_graph.edges_map) == 0
    assert len(global_graph.backward_edges_map) == 0
    assert len(global_graph.node_type_counter) == 0
    assert len(global_graph.node_name_to_ref) == 0
    assert len(global_graph.ref_to_node_name) == 0
