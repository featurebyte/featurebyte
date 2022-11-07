"""
Unit tests for query graph operation structure extraction
"""
import pytest
from bson.objectid import ObjectId

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.binary import GreaterThanWithScalarInputNode
from featurebyte.query_graph.node.generic import InputNode, ProjectNode


@pytest.fixture(name="input_event_data_node")
def input_event_data_node_fixture():
    """Input node fixture"""
    return InputNode(
        name="input_1",
        parameters={
            "columns": [
                "col_int",
                "col_float",
                "col_char",
                "event_timestamp",
                "event_id",
                "created_at",
                "cust_id",
            ],
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
            "feature_store_details": {
                "type": "snowflake",
                "details": {
                    "account": "sf_account",
                    "warehouse": "sf_warehouse",
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                },
            },
            "type": "event_data",
            "timestamp": "event_timestamp",
            "id": ObjectId(),
        },
        output_type="frame",
    )


@pytest.fixture(name="input_item_data_node")
def input_item_data_node_fixture():
    """Input node fixture"""
    return InputNode(
        name="input_2",
        parameters={
            "columns": [
                "event_id",
                "item_name",
                "item_type",
                "item_id",
            ],
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
            "feature_store_details": {
                "type": "snowflake",
                "details": {
                    "account": "sf_account",
                    "warehouse": "sf_warehouse",
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                },
            },
            "type": "item_data",
            "item_id": "item_id",
            "id": ObjectId(),
        },
        output_type="frame",
    )


@pytest.fixture(name="project_col_int_node")
def project_col_int_node_fixture():
    """Project node fixture"""
    return ProjectNode(name="project_1", parameters={"columns": ["col_int"]}, output_type="series")


@pytest.fixture(name="project_timestamp_node")
def project_timestamp_node_fixture():
    """Project node fixture"""
    return ProjectNode(
        name="project_2", parameters={"columns": ["event_timestamp"]}, output_type="series"
    )


@pytest.fixture(name="project_cust_id_node")
def project_cust_id_node_fixture():
    """Project node fixture"""
    return ProjectNode(name="project_3", parameters={"columns": ["cust_id"]}, output_type="series")


@pytest.fixture(name="project_subset_node")
def project_subset_node_fixture():
    """Project node fixture"""
    return ProjectNode(
        name="project_4", parameters={"columns": ["col_int", "cust_id"]}, output_type="frame"
    )


@pytest.fixture(name="greater_node")
def greater_node_fixture():
    """Project node fixture"""
    return GreaterThanWithScalarInputNode(
        name="gt_1", parameters={"value": 0}, output_type="series"
    )


@pytest.fixture(name="common_column_params")
def common_column_params_fixture(input_event_data_node):
    """Common column parameters fixture"""
    return {
        "tabular_data_id": input_event_data_node.parameters.id,
        "tabular_data_type": "event_data",
        "type": "source",
    }


def test_extract_operation__single_input_node(input_event_data_node):
    """Test extract_operation_structure: single input node"""
    graph = QueryGraph(nodes=[input_event_data_node], edges=[])
    op_struct = graph.extract_operation_structure(node=input_event_data_node)
    expected_columns = [
        {
            "name": col,
            "tabular_data_id": input_event_data_node.parameters.id,
            "tabular_data_type": "event_data",
            "type": "source",
        }
        for col in input_event_data_node.parameters.columns
    ]
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"


def test_extract_operation__projection(
    input_event_data_node, project_col_int_node, project_subset_node, common_column_params
):
    """Test extract_operation_structure: project"""
    graph = QueryGraph(
        nodes=[input_event_data_node, project_col_int_node],
        edges=[{"source": input_event_data_node.name, "target": project_col_int_node.name}],
    )
    op_struct = graph.extract_operation_structure(node=project_col_int_node)
    assert op_struct.columns == [{"name": "col_int", **common_column_params}]
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "series"

    graph = QueryGraph(
        nodes=[input_event_data_node, project_subset_node],
        edges=[{"source": input_event_data_node.name, "target": project_subset_node.name}],
    )
    op_struct = graph.extract_operation_structure(node=project_subset_node)
    assert op_struct.columns == [
        {"name": "col_int", **common_column_params},
        {"name": "cust_id", **common_column_params},
    ]
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"


def test_extract_operation__greater_than_and_filter(
    input_event_data_node, project_col_int_node, greater_node
):
    """Test extract_operation_structure: greater than & filter nodes"""
    graph = QueryGraph(
        nodes=[input_event_data_node, project_col_int_node, greater_node],
        edges=[
            {"source": input_event_data_node.name, "target": project_col_int_node.name},
            {"source": project_col_int_node.name, "target": greater_node.name},
        ],
    )
    op_struct = graph.extract_operation_structure(node=greater_node)
    assert op_struct.columns == [
        {
            "name": None,
            "columns": [
                {
                    "name": "col_int",
                    "tabular_data_id": input_event_data_node.parameters.id,
                    "tabular_data_type": "event_data",
                    "type": "source",
                }
            ],
            "transforms": [{"node_type": "gt", "parameters": {"value": 0}}],
            "type": "derived",
        }
    ]
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "series"

    filter_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_event_data_node, greater_node],
    )
    op_struct = graph.extract_operation_structure(node=filter_node)
    expected_columns = [
        {
            "name": col,
            "transforms": [{"node_type": "filter", "parameters": {}}],
            "columns": [
                {
                    "name": col,
                    "tabular_data_id": input_event_data_node.parameters.id,
                    "tabular_data_type": "event_data",
                    "type": "source",
                }
            ],
            "type": "derived",
        }
        for col in input_event_data_node.parameters.columns
    ]
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"


def test_extract_operation__assign(input_event_data_node, project_col_int_node):
    """Test extract_operation_structure: assign"""
    graph = QueryGraph(
        nodes=[input_event_data_node, project_col_int_node],
        edges=[
            {"source": input_event_data_node.name, "target": project_col_int_node.name},
        ],
    )
    add_node = graph.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[project_col_int_node],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "col_int_plus_one"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_event_data_node, add_node],
    )
    op_struct_input = graph.extract_operation_structure(node=input_event_data_node)
    op_struct = graph.extract_operation_structure(node=assign_node)
    assert op_struct.columns == op_struct_input.columns + [
        {
            "name": "col_int_plus_one",
            "columns": [
                {
                    "name": "col_int",
                    "tabular_data_id": input_event_data_node.parameters.id,
                    "tabular_data_type": "event_data",
                    "type": "source",
                }
            ],
            "transforms": [
                {"node_type": "add", "parameters": {"value": 1}},
                {"node_type": "assign", "parameters": {"name": "col_int_plus_one", "value": None}},
            ],
            "type": "derived",
        }
    ]
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"


def test_extract_operation__lag(
    input_event_data_node,
    project_col_int_node,
    project_timestamp_node,
    project_cust_id_node,
    common_column_params,
):
    """Test extract_operation_structure: lag"""
    graph = QueryGraph(
        nodes=[
            input_event_data_node,
            project_col_int_node,
            project_timestamp_node,
            project_cust_id_node,
        ],
        edges=[
            {"source": input_event_data_node.name, "target": project_col_int_node.name},
            {"source": input_event_data_node.name, "target": project_timestamp_node.name},
            {"source": input_event_data_node.name, "target": project_cust_id_node.name},
        ],
    )
    lag_node = graph.add_operation(
        node_type=NodeType.LAG,
        node_params={
            "entity_columns": ["cust_id"],
            "timestamp_column": "event_timestamp",
            "offset": 1,
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[project_col_int_node, project_cust_id_node, project_timestamp_node],
    )
    op_struct = graph.extract_operation_structure(node=lag_node)
    assert op_struct.columns == [
        {
            "name": None,
            "columns": [
                {"name": "col_int", **common_column_params},
                {"name": "cust_id", **common_column_params},
                {"name": "event_timestamp", **common_column_params},
            ],
            "transforms": [
                {
                    "node_type": "lag",
                    "parameters": {
                        "entity_columns": ["cust_id"],
                        "timestamp_column": "event_timestamp",
                        "offset": 1,
                    },
                },
            ],
            "type": "derived",
        }
    ]
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "series"


def test_extract_operation__groupby(input_event_data_node, common_column_params):
    """Test extract_operation_structure: groupby"""
    graph = QueryGraph(nodes=[input_event_data_node], edges=[])
    groupby_node = graph.add_groupby_operation(
        node_params={
            "keys": ["cust_id"],
            "parent": "col_int",
            "agg_func": "sum",
            "value_by": None,
            "windows": ["30m", "1h"],
            "timestamp": "event_timestamp",
            "blind_spot": 60,
            "time_modulo_frequency": 0,
            "frequency": 60,
            "names": ["feat_30m", "feat_1h"],
            "serving_names": ["cust_id"],
        },
        input_node=input_event_data_node,
    )
    op_struct = graph.extract_operation_structure(node=groupby_node)
    common_aggregation_params = {
        "groupby": ["cust_id"],
        "method": "sum",
        "columns": [
            {"name": "col_int", **common_column_params},
            {"name": "event_timestamp", **common_column_params},
            {"name": "cust_id", **common_column_params},
        ],
        "category": None,
        "groupby_type": "groupby",
        "type": "aggregation",
    }
    assert op_struct.columns == [
        {"name": "col_int", **common_column_params},
        {"name": "event_timestamp", **common_column_params},
        {"name": "cust_id", **common_column_params},
    ]
    assert op_struct.aggregations == [
        {"name": "feat_30m", "window": "30m", **common_aggregation_params},
        {"name": "feat_1h", "window": "1h", **common_aggregation_params},
    ]
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "frame"


def test_extract_operation__item_groupby(input_item_data_node):
    """Test extract_operation_structure: groupby"""
    graph = QueryGraph(nodes=[input_item_data_node], edges=[])
    groupby_node = graph.add_operation(
        node_type=NodeType.ITEM_GROUPBY,
        node_params={
            "keys": ["event_id"],
            "parent": "item_id",
            "agg_func": "count",
            "names": ["item_count"],
            "serving_names": ["event_id"],
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_item_data_node],
    )
    op_struct = graph.extract_operation_structure(node=groupby_node)
    common_column_params = {
        "tabular_data_id": input_item_data_node.parameters.id,
        "tabular_data_type": "item_data",
        "type": "source",
    }
    assert op_struct.columns == [
        {"name": "event_id", **common_column_params},
        {"name": "item_id", **common_column_params},
    ]
    assert op_struct.aggregations == [
        {
            "name": "item_count",
            "category": None,
            "columns": [
                {"name": "event_id", **common_column_params},
                {"name": "item_id", **common_column_params},
            ],
            "groupby": ["event_id"],
            "method": "count",
            "type": "aggregation",
            "groupby_type": "item_groupby",
            "window": None,
        }
    ]
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "frame"


def test_extract_operation__join(input_event_data_node, input_item_data_node):
    """Test extract_operation_structure: join"""
    graph = QueryGraph(nodes=[input_event_data_node, input_item_data_node], edges=[])
    join_node = graph.add_operation(
        node_type=NodeType.JOIN,
        node_params={
            "left_on": "event_id",
            "right_on": "event_id",
            "left_input_columns": input_event_data_node.parameters.columns,
            "right_input_columns": input_item_data_node.parameters.columns,
            "left_output_columns": ["event_id", "col_int"],
            "right_output_columns": ["item_id", "item_type"],
            "join_type": "left",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_event_data_node, input_item_data_node],
    )
    op_struct = graph.extract_operation_structure(node=join_node)
    common_event_data_column_params = {
        "tabular_data_id": input_event_data_node.parameters.id,
        "tabular_data_type": "event_data",
        "type": "source",
    }
    common_item_data_column_params = {
        "tabular_data_id": input_item_data_node.parameters.id,
        "tabular_data_type": "item_data",
        "type": "source",
    }
    assert op_struct.columns == [
        {"name": "col_int", **common_event_data_column_params},
        {"name": "event_id", **common_event_data_column_params},
        {"name": "item_type", **common_item_data_column_params},
        {"name": "item_id", **common_item_data_column_params},
    ]
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"
