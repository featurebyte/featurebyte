"""
Unit tests for query graph operation structure extraction
"""
from featurebyte.query_graph.enum import NodeOutputType, NodeType


def extract_column_parameters(input_node, other_node_names=None):
    """Extract common column parameters"""
    node_names = {input_node.name}
    if other_node_names:
        node_names.update(other_node_names)
    return {
        "tabular_data_id": input_node.parameters.id,
        "tabular_data_type": input_node.parameters.type,
        "filter": False,
        "type": "source",
        "node_names": node_names,
    }


def test_extract_operation__single_input_node(global_graph, input_node):
    """Test extract_operation_structure: single input node"""
    op_struct = global_graph.extract_operation_structure(node=input_node)
    expected_columns = [
        {"name": col, **extract_column_parameters(input_node)}
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


def test_extract_operation__project_add_assign(query_graph_and_assign_node):
    """Test extract_operation_structure: project"""
    graph, assign_node = query_graph_and_assign_node
    input_node = graph.get_node_by_name("input_1")
    common_column_params = extract_column_parameters(input_node)

    project_node = graph.get_node_by_name("project_1")
    op_struct = graph.extract_operation_structure(node=project_node)
    expected_columns = [{"name": "a", **extract_column_parameters(input_node, {"project_1"})}]
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
        {"name": "a", **extract_column_parameters(input_node, {"project_3"})},
        {"name": "b", **extract_column_parameters(input_node, {"project_3"})},
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
                {"name": "a", **extract_column_parameters(input_node, {"project_1"})},
                {"name": "b", **extract_column_parameters(input_node, {"project_2"})},
            ],
            "transforms": ["add"],
            "filter": False,
            "type": "derived",
            "node_names": {"input_1", "add_1", "project_1", "project_2"},
        }
    ]
    assert op_struct.columns == expected_derived_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "series"

    grp_op_struct = op_struct.to_group_operation_structure()
    expected_columns = [
        {"name": "a", **extract_column_parameters(input_node, {"project_1"})},
        {"name": "b", **extract_column_parameters(input_node, {"project_2"})},
    ]
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
                {"name": "a", **extract_column_parameters(input_node, {"project_1"})},
                {"name": "b", **extract_column_parameters(input_node, {"project_2"})},
            ],
            "transforms": ["add"],
            "filter": False,
            "type": "derived",
            "node_names": {"input_1", "add_1", "assign_1", "project_1", "project_2"},
        },
    ]
    assert op_struct.columns == expected_columns + expected_derived_columns
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == [
        {"name": "ts", **common_column_params},
        {"name": "cust_id", **common_column_params},
        {"name": "a", **extract_column_parameters(input_node, {"project_1"})},
        {"name": "b", **extract_column_parameters(input_node, {"project_2"})},
    ]
    assert grp_op_struct.derived_columns == expected_derived_columns
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None


def test_extract_operation__filter(graph_four_nodes):
    """Test extract_operation_structure: filter"""
    graph, input_node, _, _, filter_node = graph_four_nodes

    op_struct = graph.extract_operation_structure(node=filter_node)
    common_column_params = extract_column_parameters(
        input_node, other_node_names={"input_1", "project_1", "eq_1", "filter_1"}
    )
    expected_columns = [{"name": "column", **common_column_params, "filter": True}]
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == [
        {"name": "column", **common_column_params, "filter": True}
    ]
    assert grp_op_struct.derived_columns == []
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
    expected_source_columns = [
        {"name": "a", **extract_column_parameters(input_node, {"project_2"})},
        {"name": "cust_id", **extract_column_parameters(input_node, {"project_1"})},
        {"name": "ts", **extract_column_parameters(input_node, {"project_3"})},
    ]
    expected_derived_columns = [
        {
            "name": None,
            "columns": expected_source_columns,
            "transforms": ["lag(entity_columns=['cust_id'], offset=1, timestamp_column='ts')"],
            "filter": False,
            "type": "derived",
            "node_names": {"input_1", "lag_1", "project_1", "project_2", "project_3"},
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
    common_column_params = extract_column_parameters(input_node)
    common_aggregation_params = {
        "groupby": ["cust_id"],
        "method": "avg",
        "column": {"name": "a", **common_column_params},
        "category": None,
        "groupby_type": "groupby",
        "filter": False,
        "type": "aggregation",
        "node_names": {"input_1", "groupby_1"},
    }
    expected_columns = [
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
    expected_aggregation = {
        "name": "a_2h_average",
        "window": "2h",
        **common_aggregation_params,
        "node_names": {"input_1", "groupby_1", "project_3"},
    }
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == [expected_aggregation]
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
    expected_filtered_aggregation = {
        "columns": [expected_aggregation],
        "filter": False,
        "name": "a_2h_average",
        "node_names": {"input_1", "eq_1", "filter_1", "project_3", "groupby_1"},
        "transforms": ["filter"],
        "type": "post_aggregation",
    }
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == [expected_filtered_aggregation]

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == expected_columns
    assert grp_op_struct.derived_columns == []
    assert grp_op_struct.aggregations == [expected_aggregation]
    assert grp_op_struct.post_aggregation == expected_filtered_aggregation


def test_extract_operation__item_groupby(
    global_graph, item_data_input_node, order_size_feature_group_node
):
    """Test extract_operation_structure: item groupby"""
    op_struct = global_graph.extract_operation_structure(node=order_size_feature_group_node)
    assert op_struct.columns == []
    assert op_struct.aggregations == [
        {
            "name": "order_size",
            "category": None,
            "column": None,
            "groupby": ["order_id"],
            "method": "count",
            "type": "aggregation",
            "window": None,
            "filter": False,
            "groupby_type": "item_groupby",
            "node_names": {"input_1", "item_groupby_1"},
        }
    ]
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "frame"


def test_extract_operation__join_node(
    global_graph,
    item_data_join_event_data_with_renames_node,
    event_data_input_node,
    item_data_input_node,
):
    """Test extract_operation_structure: join"""

    # check join & its output
    op_struct = global_graph.extract_operation_structure(
        node=item_data_join_event_data_with_renames_node
    )
    common_event_data_column_params = extract_column_parameters(
        event_data_input_node,
        other_node_names={"input_2", "join_1"},
    )
    common_item_data_column_params = extract_column_parameters(
        item_data_input_node,
        other_node_names={"input_1", "join_1"},
    )
    assert op_struct.columns == [
        {"name": "order_id", **common_event_data_column_params},
        {"name": "order_method_left", **common_event_data_column_params},
        {"name": "item_name_right", **common_item_data_column_params},
        {"name": "item_type_right", **common_item_data_column_params},
    ]
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"


def test_extract_operation__join_double_aggregations(
    global_graph,
    order_size_feature_join_node,
    order_size_agg_by_cust_id_graph,
    event_data_input_node,
):
    """Test extract_operation_structure: join feature & double aggregations"""
    _, groupby_node = order_size_agg_by_cust_id_graph

    # check join & its output
    op_struct = global_graph.extract_operation_structure(node=order_size_feature_join_node)
    common_event_data_column_params = extract_column_parameters(
        event_data_input_node,
        other_node_names={"input_2"},
    )
    order_size_column = {
        "name": "ord_size",
        "columns": [],
        "transforms": ["item_groupby", "add(value=123)"],
        "filter": False,
        "type": "derived",
        "node_names": {"project_1", "item_groupby_1", "join_feature_1", "add_1", "input_1"},
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
            "column": order_size_column,
            "filter": False,
            "groupby_type": "groupby",
            "type": "aggregation",
            "node_names": {
                "input_1",
                "input_2",
                "join_feature_1",
                "item_groupby_1",
                "add_1",
                "project_1",
                "groupby_1",
            },
        }
    ]
    assert op_struct.columns == [order_size_column]
    assert op_struct.aggregations == expected_aggregations

    grp_op_struct = op_struct.to_group_operation_structure()
    assert grp_op_struct.source_columns == []
    assert grp_op_struct.derived_columns == [order_size_column]
    assert grp_op_struct.aggregations == expected_aggregations
    assert grp_op_struct.post_aggregation is None


def test_extract_operation__lookup_feature(
    global_graph,
    lookup_feature_node,
    dimension_data_input_node,
):
    """Test extract_operation_structure: lookup features"""

    op_struct = global_graph.extract_operation_structure(node=lookup_feature_node)
    common_data_params = extract_column_parameters(dimension_data_input_node)
    expected_columns = [
        {"name": "cust_value_1", **common_data_params},
        {"name": "cust_value_2", **common_data_params},
    ]
    expected_aggregations = [
        {
            "columns": [
                {
                    "category": None,
                    "column": expected_columns[0],
                    "filter": False,
                    "groupby": ["cust_id"],
                    "groupby_type": "lookup",
                    "method": None,
                    "name": "CUSTOMER ATTRIBUTE 1",
                    "node_names": {"lookup_1", "project_1", "input_1"},
                    "type": "aggregation",
                    "window": None,
                },
                {
                    "category": None,
                    "column": expected_columns[1],
                    "filter": False,
                    "groupby": ["cust_id"],
                    "groupby_type": "lookup",
                    "method": None,
                    "name": "CUSTOMER ATTRIBUTE 2",
                    "node_names": {"lookup_1", "project_2", "input_1"},
                    "type": "aggregation",
                    "window": None,
                },
            ],
            "filter": False,
            "name": "MY FEATURE",
            "node_names": {"add_1", "alias_1", "input_1", "lookup_1", "project_1", "project_2"},
            "transforms": ["add"],
            "type": "post_aggregation",
        }
    ]
    assert op_struct.columns == expected_columns
    assert op_struct.aggregations == expected_aggregations
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "series"


def test_extract_operation__alias(global_graph, input_node):
    """Test extract_operation_structure: alias"""
    project_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    add_node = global_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 10},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[project_node],
    )
    op_struct = global_graph.extract_operation_structure(node=add_node)
    expected_derived_columns = {
        "name": None,
        "columns": [{"name": "a", **extract_column_parameters(input_node, {"project_1"})}],
        "transforms": ["add(value=10)"],
        "filter": False,
        "type": "derived",
        "node_names": {"input_1", "project_1", "add_1"},
    }
    assert op_struct.columns == [expected_derived_columns]
    alias_node = global_graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "some_value"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[add_node],
    )
    op_struct = global_graph.extract_operation_structure(node=alias_node)
    assert op_struct.columns == [
        {
            **expected_derived_columns,
            "name": "some_value",
            "node_names": {"input_1", "project_1", "add_1", "alias_1"},
        }
    ]


def test_extract_operation__complicated_assignment_case_1(dataframe):
    """Test node names value tracks column lineage properly"""
    dataframe["diff"] = dataframe["TIMESTAMP_VALUE"] - dataframe["TIMESTAMP_VALUE"]
    diff = dataframe["diff"]
    dataframe["diff"] = 123
    dataframe["NEW_TIMESTAMP"] = dataframe["TIMESTAMP_VALUE"] + diff
    new_ts = dataframe["NEW_TIMESTAMP"]

    # check extract operation structure
    graph = dataframe.graph
    input_node = graph.get_node_by_name("input_1")
    common_data_column_params = extract_column_parameters(input_node)

    op_struct = graph.extract_operation_structure(node=new_ts.node)
    expected_new_ts = {
        "name": "NEW_TIMESTAMP",
        "columns": [
            {
                "name": "TIMESTAMP_VALUE",
                **extract_column_parameters(input_node, {"project_1"}),
            }
        ],
        "transforms": ["date_diff", "date_add"],
        "type": "derived",
        "filter": False,
        "node_names": {
            "input_1",
            "date_diff_1",
            "assign_1",
            "date_add_1",
            "assign_3",
            "project_1",
            "project_2",
            "project_3",
        },
    }
    assert op_struct.columns == [expected_new_ts]

    # assign_2 is not included in the lineage as `dataframe["diff"] = 123` does not affect
    # final value of NEW_TIMESTAMP column
    assert graph.get_node_by_name("assign_1").parameters == {"name": "diff", "value": None}
    assert graph.get_node_by_name("assign_3").parameters == {"name": "NEW_TIMESTAMP", "value": None}
    # check on constant value assignment
    op_struct = graph.extract_operation_structure(node=dataframe["diff"].node)
    assert op_struct.columns == [
        {
            "name": "diff",
            "columns": [],
            "transforms": [],
            "type": "derived",
            "filter": False,
            "node_names": {"assign_2", "project_4"},
        }
    ]
    assert graph.get_node_by_name("assign_2").parameters == {"name": "diff", "value": 123}

    # check frame
    op_struct = graph.extract_operation_structure(node=dataframe.node)
    expected_new_ts["node_names"].remove("project_3")
    assert op_struct.columns == [
        {"name": "CUST_ID", **common_data_column_params},
        {"name": "PRODUCT_ACTION", **common_data_column_params},
        {"name": "VALUE", **common_data_column_params},
        {"name": "MASK", **common_data_column_params},
        {"name": "TIMESTAMP_VALUE", **common_data_column_params},
        {
            "name": "diff",
            "columns": [],
            "transforms": [],
            "type": "derived",
            "filter": False,
            "node_names": {"assign_2"},
        },
        expected_new_ts,
    ]


def test_extract_operation__complicated_assignment_case_2(dataframe):
    """Test node names value tracks column lineage properly"""
    dataframe["diff"] = dataframe["VALUE"] - dataframe["CUST_ID"]
    dataframe["diff"] = dataframe["diff"] + dataframe["CUST_ID"]
    dataframe["another_diff"] = dataframe["diff"]
    dataframe["diff"] = dataframe["another_diff"]

    # check extract operation structure
    graph = dataframe.graph
    input_node = graph.get_node_by_name("input_1")
    op_struct = graph.extract_operation_structure(node=dataframe["diff"].node)
    assert op_struct == {
        "aggregations": [],
        "columns": [
            {
                "columns": [
                    {
                        "name": "VALUE",
                        **extract_column_parameters(input_node, {"project_1"}),
                    },
                    {
                        "name": "CUST_ID",
                        **extract_column_parameters(input_node, {"project_2"}),
                    },
                ],
                "filter": False,
                "name": "diff",
                "node_names": {
                    "input_1",
                    "sub_1",
                    "add_1",
                    "assign_1",
                    "assign_2",
                    "assign_3",
                    "assign_4",
                    "project_1",
                    "project_2",
                    "project_3",
                    "project_4",
                    "project_5",
                    "project_6",
                },
                "transforms": ["sub", "add"],
                "type": "derived",
            }
        ],
        "output_category": "view",
        "output_type": "series",
        "is_time_based": False,
    }
