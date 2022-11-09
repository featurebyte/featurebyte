"""
Unit tests for query graph operation structure extraction
"""
from featurebyte.query_graph.enum import NodeOutputType, NodeType


def test_extract_operation__single_input_node(global_graph, input_node):
    """Test extract_operation_structure: single input node"""
    op_struct = global_graph.extract_operation_structure(node=input_node)
    expected_columns = [
        {
            "name": col,
            "tabular_data_id": input_node.parameters.id,
            "tabular_data_type": "event_data",
            "type": "source",
        }
        for col in input_node.parameters.columns
    ]
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == expected_columns
    assert grp_op_struct.derived_columns == []
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None


def extract_common_column_parameters(input_node):
    """Extract common column parameters"""
    return {
        "tabular_data_id": input_node.parameters.id,
        "tabular_data_type": input_node.parameters.type,
        "type": "source",
    }


def test_extract_operation__project_add_assign(query_graph_and_assign_node):
    """Test extract_operation_structure: project"""
    graph, assign_node = query_graph_and_assign_node
    input_node = graph.get_node_by_name("input_1")
    common_column_params = extract_common_column_parameters(input_node)

    project_node = graph.get_node_by_name("project_1")
    op_struct = graph.extract_operation_structure(node=project_node)
    expected_columns = [{"name": project_node.parameters.columns[0], **common_column_params}]
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "series"

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == expected_columns
    assert grp_op_struct.derived_columns == []
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None

    project_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a", "b"]},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node],
    )
    op_struct = graph.extract_operation_structure(node=project_node)
    expected_columns = [
        {"name": "a", **common_column_params},
        {"name": "b", **common_column_params},
    ]
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == expected_columns
    assert grp_op_struct.derived_columns == []
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None

    add_node = graph.get_node_by_name("add_1")
    op_struct = graph.extract_operation_structure(node=add_node)
    expected_derived_columns = [
        {
            "name": None,
            "columns": [
                {"name": "a", **common_column_params},
                {"name": "b", **common_column_params},
            ],
            "transforms": [{"node_type": "add", "parameters": {"value": None}}],
            "type": "derived",
        }
    ]
    assert op_struct.columns == expected_derived_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "series"

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == expected_columns
    assert grp_op_struct.derived_columns == expected_derived_columns
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None

    op_struct = graph.extract_operation_structure(node=assign_node)
    expected_columns = [
        {"name": "ts", **common_column_params},
        {"name": "cust_id", **common_column_params},
        {"name": "a", **common_column_params},
        {"name": "b", **common_column_params},
    ]
    expected_derived_columns = [
        {
            "name": "c",
            "columns": [
                {"name": "a", **common_column_params},
                {"name": "b", **common_column_params},
            ],
            "transforms": [
                {"node_type": "add", "parameters": {"value": None}},
                {"node_type": "assign", "parameters": {"name": "c", "value": None}},
            ],
            "type": "derived",
        },
    ]
    assert op_struct.columns == expected_columns + expected_derived_columns
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == expected_columns
    assert grp_op_struct.derived_columns == expected_derived_columns
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None


def test_extract_operation__filter(graph_four_nodes):
    """Test extract_operation_structure: filter"""
    graph, input_node, _, _, filter_node = graph_four_nodes
    op_struct = graph.extract_operation_structure(node=filter_node)
    common_column_params = extract_common_column_parameters(input_node)
    expected_columns = [
        {
            "name": "column",
            "columns": [{"name": "column", **common_column_params}],
            "transforms": [{"node_type": "filter", "parameters": {}}],
            "type": "derived",
        }
    ]
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == [{"name": "column", **common_column_params}]
    assert grp_op_struct.derived_columns == expected_columns
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None


def test_extract_operation__lag(global_graph, input_node):
    """Test extract_operation_structure: lag"""
    project_map = {}
    for col in ["cust_id", "a", "ts"]:
        project_map[col] = global_graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [col]},
            node_output_type="series",
            input_nodes=[input_node],
        )

    lag_node = global_graph.add_operation(
        node_type=NodeType.LAG,
        node_params={
            "entity_columns": ["cust_id"],
            "timestamp_column": "ts",
            "offset": 1,
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[project_map["a"], project_map["cust_id"], project_map["ts"]],
    )
    op_struct = global_graph.extract_operation_structure(node=lag_node)
    common_column_params = extract_common_column_parameters(input_node)
    expected_source_columns = [
        {"name": "a", **common_column_params},
        {"name": "cust_id", **common_column_params},
        {"name": "ts", **common_column_params},
    ]
    expected_derived_columns = [
        {
            "name": None,
            "columns": expected_source_columns,
            "transforms": [
                {
                    "node_type": "lag",
                    "parameters": {
                        "entity_columns": ["cust_id"],
                        "timestamp_column": "ts",
                        "offset": 1,
                    },
                },
            ],
            "type": "derived",
        }
    ]
    assert op_struct.columns == expected_derived_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "series"

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == expected_source_columns
    assert grp_op_struct.derived_columns == expected_derived_columns
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None


def test_extract_operation__groupby(query_graph_with_groupby):
    """Test extract_operation_structure: groupby"""
    graph = query_graph_with_groupby
    input_node = graph.get_node_by_name("input_1")
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    op_struct = graph.extract_operation_structure(node=groupby_node)
    common_column_params = extract_common_column_parameters(input_node)
    common_aggregation_params = {
        "groupby": ["cust_id"],
        "method": "avg",
        "columns": [
            {"name": "ts", **common_column_params},
            {"name": "cust_id", **common_column_params},
            {"name": "a", **common_column_params},
        ],
        "category": None,
        "groupby_type": "groupby",
        "type": "aggregation",
    }
    expected_columns = [
        {"name": "ts", **common_column_params},
        {"name": "cust_id", **common_column_params},
        {"name": "a", **common_column_params},
    ]
    expected_aggregations = [
        {"name": "a_2h_average", "window": "2h", **common_aggregation_params},
        {"name": "a_48h_average", "window": "48h", **common_aggregation_params},
    ]
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == expected_aggregations
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "frame"

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == expected_columns
    assert grp_op_struct.derived_columns == []
    assert grp_op_struct.aggregations == expected_aggregations
    assert grp_op_struct.post_aggregation is None

    # check project on feature group
    project_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_2h_average"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[groupby_node],
    )
    op_struct = graph.extract_operation_structure(node=project_node)
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == [
        {"name": "a_2h_average", "window": "2h", **common_aggregation_params}
    ]
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "series"

    # check filter on feature
    eq_node = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[project_node],
    )
    filter_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[project_node, eq_node],
    )
    op_struct = graph.extract_operation_structure(node=filter_node)
    expected_post_aggregation = {
        "name": "a_2h_average",
        "type": "post_aggregation",
        "transforms": [{"node_type": "filter", "parameters": {}}],
        "columns": [expected_aggregations[0]],
    }
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == [expected_post_aggregation]

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == expected_columns
    assert grp_op_struct.derived_columns == []
    assert grp_op_struct.aggregations == [expected_aggregations[0]]
    assert grp_op_struct.post_aggregation == expected_post_aggregation


def test_extract_operation__item_groupby(
    global_graph, item_data_input_node, order_size_feature_group_node
):
    """Test extract_operation_structure: item groupby"""
    op_struct = global_graph.extract_operation_structure(node=order_size_feature_group_node)
    common_column_params = extract_common_column_parameters(item_data_input_node)
    assert op_struct.columns == [{"name": "order_id", **common_column_params}]
    assert op_struct.aggregations == [
        {
            "name": "order_size",
            "category": None,
            "columns": [{"name": "order_id", **common_column_params}],
            "groupby": ["order_id"],
            "method": "count",
            "type": "aggregation",
            "groupby_type": "item_groupby",
            "window": None,
        }
    ]
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "frame"


def test_extract_operation__join_double_aggregations(
    global_graph,
    order_size_feature_join_node,
    order_size_agg_by_cust_id_graph,
    event_data_input_node,
    item_data_input_node,
):
    """Test extract_operation_structure: join & double aggregations"""
    _, groupby_node = order_size_agg_by_cust_id_graph

    # check join & its output
    op_struct = global_graph.extract_operation_structure(node=order_size_feature_join_node)
    common_event_data_column_params = extract_common_column_parameters(event_data_input_node)
    common_item_data_column_params = extract_common_column_parameters(item_data_input_node)
    order_size_column = {
        "name": "order_size",
        "columns": [{"name": "order_id", **common_item_data_column_params}],
        "transforms": [
            {
                "node_type": "item_groupby",
                "parameters": {
                    "agg_func": "count",
                    "keys": ["order_id"],
                    "names": ["order_size"],
                    "parent": None,
                    "serving_names": ["order_id"],
                },
            }
        ],
        "type": "derived",
    }
    assert op_struct.columns == [
        {"name": "ts", **common_event_data_column_params},
        {"name": "cust_id", **common_event_data_column_params},
        {"name": "order_id", **common_event_data_column_params},
        {"name": "order_method", **common_event_data_column_params},
        order_size_column,
    ]
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"

    # check double aggregations & its output
    op_struct = global_graph.extract_operation_structure(node=groupby_node)
    expected_aggregations = [
        {
            "name": "order_size_30d_avg",
            "groupby": ["cust_id"],
            "method": "avg",
            "window": "30d",
            "category": None,
            "columns": [
                {"name": "ts", **common_event_data_column_params},
                {"name": "cust_id", **common_event_data_column_params},
                order_size_column,
            ],
            "groupby_type": "groupby",
            "type": "aggregation",
        }
    ]
    assert op_struct.columns == [
        {"name": "ts", **common_event_data_column_params},
        {"name": "cust_id", **common_event_data_column_params},
        order_size_column,
    ]
    assert op_struct.aggregations == expected_aggregations

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == [
        {"name": "ts", **common_event_data_column_params},
        {"name": "cust_id", **common_event_data_column_params},
        {"name": "order_id", **common_item_data_column_params},
    ]
    assert grp_op_struct.derived_columns == [order_size_column]
    assert grp_op_struct.aggregations == expected_aggregations
    assert grp_op_struct.post_aggregation is None
