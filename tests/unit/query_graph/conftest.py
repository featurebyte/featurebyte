"""
Common test fixtures used across unit test directories related to query_graph
"""
import pytest

from featurebyte.core.frame import Frame
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, GlobalQueryGraphState, Node
from featurebyte.query_graph.util import get_tile_table_identifier


@pytest.fixture(name="global_graph")
def global_query_graph():
    """
    Empty query graph fixture
    """
    GlobalQueryGraphState.reset()
    yield GlobalQueryGraph()


@pytest.fixture(name="query_graph_and_assign_node")
def query_graph_and_assign_node_fixture(global_graph):
    """Fixture of a query with some operations ready to run groupby"""
    # pylint: disable=duplicate-code
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": ["ts", "cust_id", "a", "b"],
            "timestamp": "ts",
            "dbtable": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "event_table",
            },
            "feature_store": {
                "type": "snowflake",
                "details": {
                    "database": "db",
                    "sf_schema": "public",
                },
            },
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_a = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    proj_b = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    sum_node = global_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a, proj_b],
    )
    assign_node = global_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "c"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, sum_node],
    )
    return global_graph, assign_node


@pytest.fixture(name="groupby_node_params")
def groupby_node_params_fixture():
    """Fixture groupby node parameters"""
    node_params = {
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
    return node_params


@pytest.fixture(name="query_graph_with_groupby")
def query_graph_with_groupby_fixture(query_graph_and_assign_node, groupby_node_params):
    """Fixture of a query graph with a groupby operation"""
    graph, assign_node = query_graph_and_assign_node
    node_params = groupby_node_params
    graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            **node_params,
            "tile_id": get_tile_table_identifier(
                graph.node_name_to_ref[assign_node.name], node_params
            ),
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    return graph


@pytest.fixture(name="query_graph_with_category_groupby")
def query_graph_with_category_groupby_fixture(query_graph_and_assign_node, groupby_node_params):
    """Fixture of a query graph with a groupby operation"""
    graph, assign_node = query_graph_and_assign_node
    node_params = groupby_node_params
    node_params["value_by"] = "product_type"
    graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            **node_params,
            "tile_id": get_tile_table_identifier(
                graph.node_name_to_ref[assign_node.name], node_params
            ),
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    return graph


@pytest.fixture(name="complex_feature_query_graph")
def complex_feature_query_graph_fixture(query_graph_with_groupby):
    """Fixture of a query graph with two independent groupby operations"""
    graph = query_graph_with_groupby
    node_params = {
        "keys": ["biz_id"],
        "value_by": None,
        "parent": "a",
        "agg_func": "sum",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["a"],
        "windows": ["a_7d_sum_by_business"],
        "serving_names": ["BUSINESS_ID"],
    }
    assign_node = graph.get_node_by_name("assign_1")
    groupby_1 = graph.get_node_by_name("groupby_1")
    groupby_2 = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            **node_params,
            "tile_id": get_tile_table_identifier(
                graph.node_name_to_ref[assign_node.name], node_params
            ),
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    feature_proj_1 = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_2h_average"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[groupby_1],
    )
    feature_proj_2 = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_7d_sum_by_business"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[groupby_2],
    )
    complex_feature_node = graph.add_operation(
        node_type=NodeType.DIV,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[feature_proj_1, feature_proj_2],
    )
    return complex_feature_node, graph


@pytest.fixture(name="graph_single_node")
def query_graph_single_node(global_graph):
    """
    Query graph with a single node
    """
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    pruned_graph, node_name_map = global_graph.prune(target_node=node_input, target_columns=set())
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_input.name])
    assert mapped_node.name == "input_1"
    graph_dict = global_graph.dict()
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
    yield global_graph, node_input


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
    pruned_graph, node_name_map = graph.prune(target_node=node_proj, target_columns={"a"})
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_proj.name])
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
    pruned_graph, node_name_map = graph.prune(target_node=node_eq, target_columns=set())
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_eq.name])
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
    pruned_graph, node_name_map = graph.prune(target_node=node_filter, target_columns=set())
    mapped_node = pruned_graph.get_node_by_name(node_name_map[node_filter.name])
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


@pytest.fixture(name="dataframe")
def dataframe_fixture(global_graph, snowflake_feature_store):
    """
    Frame test fixture
    """
    column_var_type_map = {
        "CUST_ID": DBVarType.INT,
        "PRODUCT_ACTION": DBVarType.VARCHAR,
        "VALUE": DBVarType.FLOAT,
        "MASK": DBVarType.BOOL,
    }
    node = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": list(column_var_type_map.keys()),
            "timestamp": "VALUE",
            "dbtable": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "transaction",
            },
            "feature_store": {
                "type": "snowflake",
                "details": {
                    "database": "db",
                    "sf_schema": "public",
                },
            },
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    yield Frame(
        tabular_source=(
            snowflake_feature_store,
            {"database_name": "db", "schema_name": "public", "table_name": "some_table_name"},
        ),
        node=node,
        column_var_type_map=column_var_type_map,
        column_lineage_map={col: (node.name,) for col in column_var_type_map},
        row_index_lineage=(node.name,),
    )
