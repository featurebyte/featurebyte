"""
Unit test for query graph
"""
import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, GlobalQueryGraphState, Node, QueryGraph
from featurebyte.query_graph.interpreter import GraphInterpreter


@pytest.fixture(name="graph_single_node")
def query_graph_single_node(graph):
    """
    Query graph with a single node
    """
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    pruned_graph, mapped_node = graph.prune(target_node=node_input, target_columns=set())
    assert mapped_node.name == "input_1"
    graph_dict = graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert graph_dict["nodes"] == {
        "input_1": {
            "name": "input_1",
            "type": "input",
            "parameters": {},
            "output_type": "frame",
        }
    }
    assert graph_dict["edges"] == {}
    assert node_input == Node(name="input_1", type="input", parameters={}, output_type="frame")
    yield graph, node_input


@pytest.fixture(name="graph_two_nodes")
def query_graph_two_nodes(graph_single_node):
    """
    Query graph with two nodes
    """
    graph, node_input = graph_single_node
    node_proj = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    pruned_graph, mapped_node = graph.prune(target_node=node_proj, target_columns={"a"})
    assert mapped_node.name == "project_1"
    graph_dict = graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert graph_dict["nodes"] == {
        "input_1": {"name": "input_1", "type": "input", "parameters": {}, "output_type": "frame"},
        "project_1": {
            "name": "project_1",
            "type": "project",
            "parameters": {"columns": ["a"]},
            "output_type": "series",
        },
    }
    assert graph_dict["edges"] == {"input_1": ["project_1"]}
    assert node_proj == Node(
        name="project_1", type="project", parameters={"columns": ["a"]}, output_type="series"
    )
    yield graph, node_input, node_proj


@pytest.fixture(name="graph_three_nodes")
def query_graph_three_nodes(graph_two_nodes):
    """
    Query graph with three nodes
    """
    graph, node_input, node_proj = graph_two_nodes
    node_eq = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_proj],
    )
    pruned_graph, mapped_node = graph.prune(target_node=node_eq, target_columns=set())
    assert mapped_node.name == "eq_1"
    graph_dict = graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert graph_dict["nodes"] == {
        "input_1": {"name": "input_1", "type": "input", "parameters": {}, "output_type": "frame"},
        "project_1": {
            "name": "project_1",
            "type": "project",
            "parameters": {"columns": ["a"]},
            "output_type": "series",
        },
        "eq_1": {"name": "eq_1", "type": "eq", "parameters": {"value": 1}, "output_type": "series"},
    }
    assert graph_dict["edges"] == {"input_1": ["project_1"], "project_1": ["eq_1"]}
    assert node_eq == Node(name="eq_1", type="eq", parameters={"value": 1}, output_type="series")
    yield graph, node_input, node_proj, node_eq


@pytest.fixture(name="graph_four_nodes")
def query_graph_four_nodes(graph_three_nodes):
    """
    Query graph with four nodes
    """
    graph, node_input, node_proj, node_eq = graph_three_nodes
    node_filter = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, node_eq],
    )
    pruned_graph, mapped_node = graph.prune(target_node=node_filter, target_columns=set())
    assert mapped_node.name == "filter_1"
    graph_dict = graph.dict()
    assert graph_dict == pruned_graph.dict()
    assert graph_dict["nodes"] == {
        "input_1": {"name": "input_1", "type": "input", "parameters": {}, "output_type": "frame"},
        "project_1": {
            "name": "project_1",
            "type": "project",
            "parameters": {"columns": ["a"]},
            "output_type": "series",
        },
        "eq_1": {"name": "eq_1", "type": "eq", "parameters": {"value": 1}, "output_type": "series"},
        "filter_1": {
            "name": "filter_1",
            "type": "filter",
            "parameters": {},
            "output_type": "frame",
        },
    }
    assert graph_dict["edges"] == {
        "input_1": ["project_1", "filter_1"],
        "project_1": ["eq_1"],
        "eq_1": ["filter_1"],
    }
    assert node_filter == Node(name="filter_1", type="filter", parameters={}, output_type="frame")
    yield graph, node_input, node_proj, node_eq, node_filter


def test_add_operation__add_duplicated_node_on_two_nodes_graph(graph_two_nodes):
    """
    Test add operation by adding a duplicated node on a 2-node graph
    """
    graph, node_input, node_proj = graph_two_nodes
    node_duplicated = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    graph_dict = graph.dict()
    assert graph_dict["nodes"] == {
        "input_1": {"name": "input_1", "type": "input", "parameters": {}, "output_type": "frame"},
        "project_1": {
            "name": "project_1",
            "type": "project",
            "parameters": {"columns": ["a"]},
            "output_type": "series",
        },
    }
    assert graph_dict["edges"] == {"input_1": ["project_1"]}
    assert node_duplicated == node_proj


def test_add_operation__add_duplicated_node_on_four_nodes_graph(graph_four_nodes):
    """
    Test add operation by adding a duplicated node on a 4-node graph
    """
    graph, _, node_proj, node_eq, _ = graph_four_nodes
    node_duplicated = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_proj],
    )
    graph_dict = graph.dict()
    assert graph_dict["nodes"] == {
        "input_1": {"name": "input_1", "type": "input", "parameters": {}, "output_type": "frame"},
        "project_1": {
            "name": "project_1",
            "type": "project",
            "parameters": {"columns": ["a"]},
            "output_type": "series",
        },
        "eq_1": {"name": "eq_1", "type": "eq", "parameters": {"value": 1}, "output_type": "series"},
        "filter_1": {
            "name": "filter_1",
            "type": "filter",
            "parameters": {},
            "output_type": "frame",
        },
    }
    assert graph_dict["edges"] == {
        "input_1": ["project_1", "filter_1"],
        "project_1": ["eq_1"],
        "eq_1": ["filter_1"],
    }
    assert node_duplicated == node_eq


def test_prune__redundant_assign_nodes(dataframe):
    """
    Test graph pruning on a query graph with redundant assign nodes
    """
    dataframe["redundantA"] = dataframe["CUST_ID"] / 10
    dataframe["redundantB"] = dataframe["VALUE"] + 10
    dataframe["target"] = dataframe["CUST_ID"] * dataframe["VALUE"]
    assert dataframe.node == Node(
        name="assign_3", type="assign", parameters={"name": "target"}, output_type="frame"
    )
    pruned_graph, mapped_node = dataframe.graph.prune(
        target_node=dataframe.node, target_columns={"target"}
    )
    assert pruned_graph.edges == {
        "input_1": ["project_1", "project_2", "assign_1"],
        "project_1": ["mul_1"],
        "project_2": ["mul_1"],
        "mul_1": ["assign_1"],
    }
    assert pruned_graph.nodes["assign_1"] == {
        "name": "assign_1",
        "type": "assign",
        "parameters": {"name": "target"},
        "output_type": "frame",
    }
    assert mapped_node.name == "assign_1"


def test_prune__redundant_assign_node_with_same_target_column_name(dataframe):
    """
    Test graph pruning on a query graph with redundant assign node of same target name
    """
    dataframe["VALUE"] = 1
    dataframe["VALUE"] = dataframe["CUST_ID"] * 10
    assert dataframe.graph.edges == {
        "input_1": ["assign_1", "project_1"],
        "project_1": ["mul_1"],
        "assign_1": ["assign_2"],
        "mul_1": ["assign_2"],
    }
    assert dataframe.graph.nodes["assign_1"]["parameters"] == {"value": 1, "name": "VALUE"}
    assert dataframe.graph.nodes["assign_2"]["parameters"] == {"name": "VALUE"}
    pruned_graph, mapped_node = dataframe.graph.prune(
        target_node=dataframe.node, target_columns={"VALUE"}
    )
    assert pruned_graph.edges == {
        "input_1": ["project_1", "assign_1"],
        "project_1": ["mul_1"],
        "mul_1": ["assign_1"],
    }
    assert pruned_graph.nodes["assign_1"]["parameters"] == {"name": "VALUE"}
    assert mapped_node.name == "assign_1"


def test_prune__redundant_project_nodes(dataframe):
    """
    Test graph pruning on a query graph with redundant project nodes
    """
    _ = dataframe["CUST_ID"]
    _ = dataframe["VALUE"]
    mask = dataframe["MASK"]
    assert dataframe.graph.edges == {"input_1": ["project_1", "project_2", "project_3"]}
    pruned_graph, mapped_node = dataframe.graph.prune(target_node=mask.node, target_columns=set())
    assert pruned_graph.edges == {"input_1": ["project_1"]}
    assert pruned_graph.nodes["project_1"]["parameters"]["columns"] == ["MASK"]
    assert mapped_node.name == "project_1"


def test_prune__multiple_non_redundant_assign_nodes__interactive_pattern(dataframe):
    """
    Test graph pruning on a query graph without any redundant assign nodes (interactive pattern)
    """
    dataframe["requiredA"] = dataframe["CUST_ID"] / 10
    dataframe["requiredB"] = dataframe["VALUE"] + 10
    dataframe["target"] = dataframe["requiredA"] * dataframe["requiredB"]
    assert dataframe.node == Node(
        name="assign_3", type="assign", parameters={"name": "target"}, output_type="frame"
    )
    pruned_graph, mapped_node = dataframe.graph.prune(
        target_node=dataframe.node, target_columns={"target"}
    )
    assert pruned_graph.edges == {
        "input_1": ["project_1", "assign_1", "project_3"],
        "project_1": ["div_1"],
        "div_1": ["assign_1"],
        "assign_1": ["project_2", "assign_2"],
        "project_3": ["add_1"],
        "add_1": ["assign_2"],
        "assign_2": ["project_4", "assign_3"],
        "project_2": ["mul_1"],
        "project_4": ["mul_1"],
        "mul_1": ["assign_3"],
    }
    assert pruned_graph.nodes["assign_1"]["parameters"]["name"] == "requiredA"
    assert pruned_graph.nodes["assign_2"]["parameters"]["name"] == "requiredB"
    assert pruned_graph.nodes["project_1"]["parameters"]["columns"] == ["CUST_ID"]
    assert pruned_graph.nodes["project_3"]["parameters"]["columns"] == ["VALUE"]
    assert pruned_graph.nodes["project_2"]["parameters"]["columns"] == ["requiredA"]
    assert pruned_graph.nodes["project_4"]["parameters"]["columns"] == ["requiredB"]
    assert mapped_node.name == "assign_3"


def test_prune__multiple_non_redundant_assign_nodes__cascading_pattern(dataframe):
    """
    Test graph pruning on a query graph without any redundant assign nodes (cascading pattern)
    """
    dataframe["requiredA"] = dataframe["CUST_ID"] / 10
    dataframe["requiredB"] = dataframe["requiredA"] + 10
    dataframe["target"] = dataframe["requiredB"] * 10
    assert dataframe.node == Node(
        name="assign_3", type="assign", parameters={"name": "target"}, output_type="frame"
    )
    pruned_graph, mapped_node = dataframe.graph.prune(
        target_node=dataframe.node, target_columns={"target"}
    )
    assert pruned_graph.edges == {
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
    assert pruned_graph.nodes["assign_1"]["parameters"]["name"] == "requiredA"
    assert pruned_graph.nodes["assign_2"]["parameters"]["name"] == "requiredB"
    assert pruned_graph.nodes["project_1"]["parameters"]["columns"] == ["CUST_ID"]
    assert pruned_graph.nodes["project_2"]["parameters"]["columns"] == ["requiredA"]
    assert pruned_graph.nodes["project_3"]["parameters"]["columns"] == ["requiredB"]
    assert mapped_node.name == "assign_3"


def test_topological_sort__empty_graph(graph):
    """
    Test topological sorting on empty graph edge case
    """
    assert graph.nodes == {}
    assert graph.edges == {}
    assert graph.topological_sort() == []


def test_topological_sort__four_nodes(graph_four_nodes):
    """
    Test topological sorting on a graph with single branch of nodes
    """
    graph, _, _, _, _ = graph_four_nodes
    sorted_node_names = graph.topological_sort()
    assert sorted_node_names == ["input_1", "project_1", "eq_1", "filter_1"]


def test_serialization_deserialization__clean_global_graph(graph_four_nodes):
    """
    Test serialization & deserialization of query graph object (clean global query graph)
    """
    graph, _, _, _, _ = graph_four_nodes
    graph_dict = graph.dict()
    deserialized_graph = QueryGraph.parse_obj(graph_dict)
    assert graph == deserialized_graph

    # clean up global query graph state & load the deserialized graph to the clean global query graph
    GlobalQueryGraphState.reset()
    new_global_graph = GlobalQueryGraph()
    assert new_global_graph.nodes == {}
    new_global_graph.load(graph)
    assert new_global_graph.dict() == graph_dict


def test_serialization_deserialization__with_existing_non_empty_graph(dataframe):
    """
    Test serialization & deserialization of query graph object (non-empty global query graph)
    """
    # construct a graph
    dataframe["feature"] = dataframe["VALUE"] * dataframe["CUST_ID"] / 100.0
    dataframe = dataframe[dataframe["MASK"]]

    # serialize the graph with the last node of the graph
    node_names = set(dataframe.graph.nodes)
    pruned_graph, mapped_node = dataframe.graph.prune(
        target_node=dataframe.node, target_columns=set(dataframe.columns)
    )
    query_before_serialization = GraphInterpreter(pruned_graph).construct_preview_sql(
        mapped_node.name
    )

    # further modify the global graph & check the global query graph are updated
    dataframe["feature"] = dataframe["VALUE"] / dataframe["CUST_ID"]
    dataframe["CUST_ID"] = dataframe["CUST_ID"] + 10
    assert set(dataframe.graph.nodes).difference(node_names) == {
        "add_1",
        "assign_2",
        "assign_3",
        "div_2",
        "project_4",
        "project_5",
    }

    # construct the query of the last node
    node_before_load, columns_before_load = dataframe.node, dataframe.columns
    pruned_graph_before_load, mapped_node_before_load = dataframe.graph.prune(
        target_node=node_before_load, target_columns=set(columns_before_load)
    )
    query_before_load = GraphInterpreter(pruned_graph_before_load).construct_preview_sql(
        mapped_node_before_load.name
    )

    # deserialize the graph, load the graph to global query graph & check the generated query
    graph = QueryGraph.parse_obj(pruned_graph.dict())
    _, node_name_map = GlobalQueryGraph().load(graph)
    node_global = GlobalQueryGraph().get_node_by_name(node_name_map[mapped_node.name])
    assert (
        GraphInterpreter(GlobalQueryGraph()).construct_preview_sql(node_global.name)
        == query_before_serialization
    )

    # check that loading the deserialized graph back to global won't affect other node
    pruned_graph_after_load, _ = GlobalQueryGraph().prune(
        target_node=node_before_load, target_columns=set(columns_before_load)
    )
    query_after_load = GraphInterpreter(pruned_graph_after_load).construct_preview_sql(
        mapped_node_before_load.name
    )
    assert query_before_load == query_after_load
