"""
Unit test for query graph
"""
from collections import defaultdict

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, GlobalQueryGraphState, QueryGraph
from featurebyte.query_graph.interpreter import GraphInterpreter
from featurebyte.query_graph.node import construct_node


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

    assert graph_dict["nodes"]["input_1"] == {
        "name": "input_1",
        "type": "input",
        "parameters": graph_dict["nodes"]["input_1"]["parameters"],
        "output_type": "frame",
    }
    assert graph_dict["nodes"]["project_1"] == {
        "name": "project_1",
        "type": "project",
        "parameters": {"columns": ["a"]},
        "output_type": "series",
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
    assert pruned_graph.edges == {
        "input_1": ["project_1", "project_2", "assign_1"],
        "project_1": ["mul_1"],
        "project_2": ["mul_1"],
        "mul_1": ["assign_1"],
    }
    assert pruned_graph.nodes["assign_1"] == {
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
    assert dataframe_dict["graph"]["edges"] == {
        "input_1": ["project_1", "assign_1"],
        "project_1": ["mul_1"],
        "mul_1": ["assign_1"],
    }
    pruned_graph, node_name_map = dataframe.graph.prune(
        target_node=dataframe.node, target_columns={"VALUE"}
    )
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    assert pruned_graph.nodes["assign_1"]["parameters"] == {"name": "VALUE", "value": None}
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
    pruned_graph, node_name_map = dataframe.graph.prune(
        target_node=dataframe.node, target_columns={"target"}
    )
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    assert pruned_graph.edges == {
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
    assert pruned_graph.nodes["assign_1"]["parameters"]["name"] == "requiredB"
    assert pruned_graph.nodes["assign_2"]["parameters"]["name"] == "requiredA"
    assert pruned_graph.nodes["project_1"]["parameters"]["columns"] == ["VALUE"]
    assert pruned_graph.nodes["project_2"]["parameters"]["columns"] == ["requiredB"]
    assert pruned_graph.nodes["project_3"]["parameters"]["columns"] == ["CUST_ID"]
    assert pruned_graph.nodes["project_4"]["parameters"]["columns"] == ["requiredA"]
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
    # pylint: disable=too-many-locals
    # construct a graph
    dataframe["feature"] = dataframe["VALUE"] * dataframe["CUST_ID"] / 100.0
    dataframe = dataframe[dataframe["MASK"]]

    # serialize the graph with the last node of the graph
    node_names = set(dataframe.graph.nodes)
    pruned_graph, node_name_map = dataframe.graph.prune(
        target_node=dataframe.node, target_columns=set(dataframe.columns)
    )
    mapped_node = pruned_graph.get_node_by_name(node_name_map[dataframe.node.name])
    query_before_serialization = GraphInterpreter(pruned_graph).construct_preview_sql(
        node_name=mapped_node.name
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
    pruned_graph_before_load, node_name_map_before_load = dataframe.graph.prune(
        target_node=node_before_load, target_columns=set(columns_before_load)
    )
    mapped_node_before_load = pruned_graph_before_load.get_node_by_name(
        node_name_map_before_load[node_before_load.name]
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
    assert isinstance(graph.edges, defaultdict)
    assert isinstance(graph.backward_edges, defaultdict)
    assert isinstance(graph.node_type_counter, defaultdict)

    # check that loading the deserialized graph back to global won't affect other node
    pruned_graph_after_load, _ = GlobalQueryGraph().prune(
        target_node=node_before_load, target_columns=set(columns_before_load)
    )
    query_after_load = GraphInterpreter(pruned_graph_after_load).construct_preview_sql(
        mapped_node_before_load.name
    )
    assert query_before_load == query_after_load
