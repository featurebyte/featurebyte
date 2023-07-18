"""
Unit tests for query graph operation structure extraction
"""

import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory
from tests.unit.query_graph.util import to_dict


def extract_column_parameters(input_node, other_node_names=None, node_name=None):
    """Extract common column parameters"""
    node_names = {input_node.name}
    if other_node_names:
        node_names.update(other_node_names)
    return {
        "table_id": input_node.parameters.id,
        "table_type": input_node.parameters.type,
        "filter": False,
        "type": "source",
        "node_names": node_names,
        "node_name": node_name or input_node.name,
    }


def test_extract_operation__single_input_node(global_graph, input_node):
    """Test extract_operation_structure: single input node"""
    op_struct = global_graph.extract_operation_structure(
        node=input_node, keep_all_source_columns=True
    )
    expected_columns = [
        {"name": col.name, "dtype": col.dtype, **extract_column_parameters(input_node)}
        for col in input_node.parameters.columns
    ]
    assert to_dict(op_struct.columns) == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"
    assert op_struct.row_index_lineage == (input_node.name,)

    grp_op_struct = op_struct.to_group_operation_structure()
    assert to_dict(grp_op_struct.source_columns) == expected_columns
    assert grp_op_struct.derived_columns == []
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None


def test_extract_operation__project_add_assign(query_graph_and_assign_node):
    """Test extract_operation_structure: project"""
    graph, assign_node = query_graph_and_assign_node
    input_node = graph.get_node_by_name("input_1")
    common_column_params = extract_column_parameters(input_node)

    project_node = graph.get_node_by_name("project_1")
    op_struct = graph.extract_operation_structure(node=project_node, keep_all_source_columns=True)
    expected_columns = [
        {"name": "a", "dtype": "FLOAT", **extract_column_parameters(input_node, {"project_1"})}
    ]
    assert to_dict(op_struct.columns) == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "series"
    assert op_struct.row_index_lineage == ("input_1",)

    grp_op_struct = op_struct.to_group_operation_structure()
    assert to_dict(grp_op_struct.source_columns) == expected_columns
    assert grp_op_struct.derived_columns == []
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None

    project_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a", "b"]},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node],
    )
    op_struct = graph.extract_operation_structure(node=project_node, keep_all_source_columns=True)
    expected_columns = [
        {"name": "a", "dtype": "FLOAT", **extract_column_parameters(input_node, {"project_3"})},
        {"name": "b", "dtype": "FLOAT", **extract_column_parameters(input_node, {"project_3"})},
    ]
    assert to_dict(op_struct.columns) == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"
    assert op_struct.row_index_lineage == ("input_1",)

    grp_op_struct = op_struct.to_group_operation_structure()
    assert to_dict(grp_op_struct.source_columns) == expected_columns
    assert grp_op_struct.derived_columns == []
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None

    add_node = graph.get_node_by_name("add_1")
    op_struct = graph.extract_operation_structure(node=add_node, keep_all_source_columns=True)
    expected_derived_columns = [
        {
            "name": None,
            "dtype": "FLOAT",
            "columns": [
                {
                    "name": "a",
                    "dtype": "FLOAT",
                    **extract_column_parameters(input_node, {"project_1"}),
                },
                {
                    "name": "b",
                    "dtype": "FLOAT",
                    **extract_column_parameters(input_node, {"project_2"}),
                },
            ],
            "transforms": ["add"],
            "filter": False,
            "type": "derived",
            "node_names": {"input_1", "add_1", "project_1", "project_2"},
            "node_name": "add_1",
        }
    ]
    assert to_dict(op_struct.columns) == expected_derived_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "series"
    assert op_struct.row_index_lineage == ("input_1",)

    grp_op_struct = op_struct.to_group_operation_structure()
    expected_columns = [
        {"name": "a", "dtype": "FLOAT", **extract_column_parameters(input_node, {"project_1"})},
        {"name": "b", "dtype": "FLOAT", **extract_column_parameters(input_node, {"project_2"})},
    ]
    assert to_dict(grp_op_struct.source_columns) == expected_columns
    assert to_dict(grp_op_struct.derived_columns) == expected_derived_columns
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None
    assert grp_op_struct.row_index_lineage == ("input_1",)

    op_struct = graph.extract_operation_structure(node=assign_node, keep_all_source_columns=True)
    expected_columns = [
        {"name": "ts", "dtype": "TIMESTAMP", **common_column_params},
        {"name": "cust_id", "dtype": "INT", **common_column_params},
        {"name": "a", "dtype": "FLOAT", **common_column_params},
        {"name": "b", "dtype": "FLOAT", **common_column_params},
    ]
    expected_derived_columns = [
        {
            "name": "c",
            "dtype": "FLOAT",
            "columns": [
                {
                    "name": "a",
                    "dtype": "FLOAT",
                    **extract_column_parameters(input_node, {"project_1"}),
                },
                {
                    "name": "b",
                    "dtype": "FLOAT",
                    **extract_column_parameters(input_node, {"project_2"}),
                },
            ],
            "transforms": ["add"],
            "filter": False,
            "type": "derived",
            "node_names": {"input_1", "add_1", "assign_1", "project_1", "project_2"},
            "node_name": "assign_1",
        },
    ]
    assert to_dict(op_struct.columns) == expected_columns + expected_derived_columns
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"
    assert op_struct.row_index_lineage == ("input_1",)

    grp_op_struct = op_struct.to_group_operation_structure()
    assert to_dict(grp_op_struct.source_columns) == [
        {"name": "ts", "dtype": "TIMESTAMP", **common_column_params},
        {"name": "cust_id", "dtype": "INT", **common_column_params},
        {"name": "a", "dtype": "FLOAT", **extract_column_parameters(input_node, {"project_1"})},
        {"name": "b", "dtype": "FLOAT", **extract_column_parameters(input_node, {"project_2"})},
    ]
    assert to_dict(grp_op_struct.derived_columns) == expected_derived_columns
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None
    assert grp_op_struct.row_index_lineage == ("input_1",)


def test_extract_operation__filter(graph_four_nodes):
    """Test extract_operation_structure: filter"""
    graph, input_node, _, _, filter_node = graph_four_nodes

    op_struct = graph.extract_operation_structure(node=filter_node, keep_all_source_columns=True)
    common_column_params = extract_column_parameters(
        input_node,
        other_node_names={"input_1", "project_1", "eq_1", "filter_1"},
        node_name="filter_1",
    )
    expected_columns = [
        {"name": "column", "dtype": "FLOAT", **common_column_params, "filter": True}
    ]
    assert to_dict(op_struct.columns) == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"
    assert op_struct.row_index_lineage == ("input_1", "filter_1")

    grp_op_struct = op_struct.to_group_operation_structure()
    assert to_dict(grp_op_struct.source_columns) == [
        {"name": "column", "dtype": "FLOAT", **common_column_params, "filter": True}
    ]
    assert grp_op_struct.derived_columns == []
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None
    assert grp_op_struct.row_index_lineage == ("input_1", "filter_1")


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
    op_struct = global_graph.extract_operation_structure(
        node=lag_node, keep_all_source_columns=True
    )
    expected_source_columns = [
        {"name": "a", "dtype": "FLOAT", **extract_column_parameters(input_node, {"project_2"})},
        {"name": "cust_id", "dtype": "INT", **extract_column_parameters(input_node, {"project_1"})},
        {
            "name": "ts",
            "dtype": "TIMESTAMP",
            **extract_column_parameters(input_node, {"project_3"}),
        },
    ]
    expected_derived_columns = [
        {
            "name": None,
            "dtype": "FLOAT",
            "columns": expected_source_columns,
            "transforms": ["lag(entity_columns=['cust_id'], offset=1, timestamp_column='ts')"],
            "filter": False,
            "type": "derived",
            "node_names": {"input_1", "lag_1", "project_1", "project_2", "project_3"},
            "node_name": "lag_1",
        }
    ]
    assert to_dict(op_struct.columns) == expected_derived_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "series"
    assert op_struct.row_index_lineage == ("input_1",)

    grp_op_struct = op_struct.to_group_operation_structure()
    assert to_dict(grp_op_struct.source_columns) == expected_source_columns
    assert to_dict(grp_op_struct.derived_columns) == expected_derived_columns
    assert grp_op_struct.aggregations == []
    assert grp_op_struct.post_aggregation is None
    assert grp_op_struct.row_index_lineage == ("input_1",)


@pytest.mark.parametrize("keep_all_source_columns", [True, False])
def test_extract_operation__groupby(query_graph_with_groupby, keep_all_source_columns):
    """Test extract_operation_structure: groupby"""
    graph = query_graph_with_groupby
    input_node = graph.get_node_by_name("input_1")
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    op_struct = graph.extract_operation_structure(
        node=groupby_node, keep_all_source_columns=keep_all_source_columns
    )
    common_column_params = extract_column_parameters(input_node)
    common_aggregation_params = {
        "keys": ["cust_id"],
        "method": "avg",
        "column": {"name": "a", "dtype": "FLOAT", **common_column_params},
        "category": None,
        "aggregation_type": "groupby",
        "filter": False,
        "type": "aggregation",
        "node_names": {"input_1", "groupby_1"},
        "node_name": "groupby_1",
    }
    expected_columns = []
    if keep_all_source_columns:
        # keep all source columns include also event_timestamp and group by columns
        expected_columns.extend(
            [
                {"name": col_name, "dtype": var_type, **common_column_params}
                for col_name, var_type in [("ts", "TIMESTAMP"), ("cust_id", "INT")]
            ]
        )
    expected_columns.append({"name": "a", "dtype": "FLOAT", **common_column_params})
    expected_aggregations = [
        {"name": "a_2h_average", "dtype": "FLOAT", "window": "2h", **common_aggregation_params},
        {"name": "a_48h_average", "dtype": "FLOAT", "window": "48h", **common_aggregation_params},
    ]
    assert to_dict(op_struct.columns) == expected_columns
    assert to_dict(op_struct.aggregations) == expected_aggregations
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "frame"
    assert op_struct.row_index_lineage == ("groupby_1",)
    assert op_struct.is_time_based is True

    grp_op_struct = op_struct.to_group_operation_structure()
    assert to_dict(grp_op_struct.source_columns) == expected_columns
    assert grp_op_struct.derived_columns == []
    assert to_dict(grp_op_struct.aggregations) == expected_aggregations
    assert grp_op_struct.post_aggregation is None
    assert grp_op_struct.row_index_lineage == ("groupby_1",)
    assert grp_op_struct.is_time_based is True

    # check project on feature group
    project_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a_2h_average"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[groupby_node],
    )
    op_struct = graph.extract_operation_structure(
        node=project_node, keep_all_source_columns=keep_all_source_columns
    )
    expected_aggregation = {
        "name": "a_2h_average",
        "window": "2h",
        **common_aggregation_params,
        "node_names": {"input_1", "groupby_1", "project_3"},
        "dtype": "FLOAT",
    }
    assert to_dict(op_struct.columns) == expected_columns
    assert to_dict(op_struct.aggregations) == [expected_aggregation]
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "series"
    assert op_struct.row_index_lineage == ("groupby_1",)

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
    op_struct = graph.extract_operation_structure(
        node=filter_node, keep_all_source_columns=keep_all_source_columns
    )
    expected_filtered_aggregation = {
        "columns": [expected_aggregation],
        "filter": False,
        "name": "a_2h_average",
        "node_names": {"input_1", "eq_1", "filter_1", "project_3", "groupby_1"},
        "node_name": "filter_1",
        "transforms": ["filter"],
        "type": "post_aggregation",
        "dtype": "FLOAT",
    }
    assert to_dict(op_struct.columns) == expected_columns
    assert to_dict(op_struct.aggregations) == [expected_filtered_aggregation]
    assert op_struct.row_index_lineage == ("groupby_1", "filter_1")

    grp_op_struct = op_struct.to_group_operation_structure()
    assert to_dict(grp_op_struct.source_columns) == expected_columns
    assert grp_op_struct.derived_columns == []
    assert to_dict(grp_op_struct.aggregations) == [expected_aggregation]
    assert to_dict(grp_op_struct.post_aggregation) == expected_filtered_aggregation
    assert grp_op_struct.row_index_lineage == ("groupby_1", "filter_1")


@pytest.fixture(name="order_id_source_data")
def order_id_source_data_fixture():
    """Order id source table"""
    return {
        "name": "order_id",
        "dtype": "INT",
        "filter": False,
        "node_names": {"input_1"},
        "node_name": "input_1",
        "table_type": "item_table",
        "table_id": None,
        "type": "source",
    }


def test_extract_operation__item_groupby(
    global_graph, item_table_input_node, order_size_feature_group_node, order_id_source_data
):
    """Test extract_operation_structure: item groupby"""
    op_struct = global_graph.extract_operation_structure(
        node=order_size_feature_group_node, keep_all_source_columns=True
    )
    assert to_dict(op_struct.columns) == [order_id_source_data]
    assert to_dict(op_struct.aggregations) == [
        {
            "name": "order_size",
            "dtype": "FLOAT",
            "category": None,
            "column": None,
            "keys": ["order_id"],
            "method": "count",
            "type": "aggregation",
            "window": None,
            "filter": False,
            "aggregation_type": "item_groupby",
            "node_names": {"input_1", "item_groupby_1"},
            "node_name": "item_groupby_1",
        }
    ]
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "frame"
    assert op_struct.row_index_lineage == ("item_groupby_1",)
    assert op_struct.is_time_based is False


@pytest.mark.parametrize("keep_all_source_columns", [True, False])
def test_extract_operation__join_node(
    global_graph,
    item_table_join_event_table_with_renames_node,
    event_table_input_node,
    item_table_input_node,
    keep_all_source_columns,
):
    """Test extract_operation_structure: join"""

    # check join & its output
    op_struct = global_graph.extract_operation_structure(
        node=item_table_join_event_table_with_renames_node,
        keep_all_source_columns=keep_all_source_columns,
    )
    common_event_table_column_params = extract_column_parameters(
        event_table_input_node,
        other_node_names={"input_2", "join_1"},
        node_name="join_1",
    )
    common_item_table_column_params = extract_column_parameters(
        item_table_input_node,
        other_node_names={"input_1", "join_1"},
        node_name="join_1",
    )
    expected_columns = [
        {"name": "order_id", "dtype": "INT", **common_event_table_column_params},
        {"name": "order_method_left", "dtype": "VARCHAR", **common_event_table_column_params},
    ]
    if keep_all_source_columns:
        # include columns that affected the row index lineage
        def _get_other_params(column_name):
            common_column_params = {"table_id": None, "filter": False, "type": "source"}
            input_1_params = {
                "node_name": "input_1",
                "node_names": {"input_1"},
                "table_type": "item_table",
            }
            input_2_params = {
                "node_name": "input_2",
                "node_names": {"input_2"},
                "table_type": "event_table",
            }
            other_params = {
                "columns": [
                    {"name": "order_id", "dtype": "INT", **common_column_params, **input_2_params},
                    {"name": "order_id", "dtype": "INT", **common_column_params, **input_1_params},
                    {
                        "name": column_name,
                        "dtype": "VARCHAR",
                        **common_column_params,
                        **input_1_params,
                    },
                ],
                "filter": False,
                "node_name": "join_1",
                "node_names": {"input_1", "join_1", "input_2"},
                "transforms": ["join"],
                "type": "derived",
            }
            return other_params

        right_columns = [
            {"name": "item_type_right", "dtype": "VARCHAR", **_get_other_params("item_type")},
            {"name": "item_name_right", "dtype": "VARCHAR", **_get_other_params("item_name")},
        ]
    else:
        # only keep the columns from the right table that are used in the join
        right_columns = [
            {"name": "item_type_right", "dtype": "VARCHAR", **common_item_table_column_params},
            {"name": "item_name_right", "dtype": "VARCHAR", **common_item_table_column_params},
        ]

    expected_columns.extend(right_columns)
    assert to_dict(op_struct.columns) == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"
    assert op_struct.row_index_lineage == ("input_2", "join_1")


@pytest.mark.parametrize("keep_all_source_columns", [True, False])
def test_extract_operation__join_double_aggregations(
    global_graph,
    order_size_feature_join_node,
    order_size_agg_by_cust_id_graph,
    event_table_input_node,
    order_id_source_data,
    keep_all_source_columns,
):
    """Test extract_operation_structure: join feature & double aggregations"""
    _, groupby_node = order_size_agg_by_cust_id_graph

    # check join & its output
    op_struct = global_graph.extract_operation_structure(
        node=order_size_feature_join_node, keep_all_source_columns=keep_all_source_columns
    )
    common_event_table_column_params = extract_column_parameters(
        event_table_input_node,
        other_node_names={"input_2"},
    )
    order_size_column = {
        "name": "ord_size",
        "columns": [order_id_source_data],
        "transforms": ["item_groupby", "add(value=123)"],
        "filter": False,
        "type": "derived",
        "node_names": {"project_1", "item_groupby_1", "join_feature_1", "add_1", "input_1"},
        "node_name": "join_feature_1",
        "dtype": "FLOAT",
    }
    assert to_dict(op_struct.columns) == [
        {"name": "ts", "dtype": "TIMESTAMP", **common_event_table_column_params},
        {"name": "cust_id", "dtype": "INT", **common_event_table_column_params},
        {"name": "order_id", "dtype": "INT", **common_event_table_column_params},
        {"name": "order_method", "dtype": "VARCHAR", **common_event_table_column_params},
        order_size_column,
    ]
    assert op_struct.aggregations == []
    assert op_struct.output_category == "view"
    assert op_struct.output_type == "frame"
    assert op_struct.row_index_lineage == ("input_2",)

    # check double aggregations & its output
    op_struct = global_graph.extract_operation_structure(
        node=groupby_node, keep_all_source_columns=keep_all_source_columns
    )
    expected_aggregations = [
        {
            "name": "order_size_30d_avg",
            "keys": ["cust_id"],
            "method": "avg",
            "window": "30d",
            "category": None,
            "column": order_size_column,
            "filter": False,
            "aggregation_type": "groupby",
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
            "node_name": "groupby_1",
            "dtype": "FLOAT",
        }
    ]
    expected_columns = []
    if keep_all_source_columns:
        expected_columns.extend(
            [
                {"name": "ts", "dtype": "TIMESTAMP", **common_event_table_column_params},
                {"name": "cust_id", "dtype": "INT", **common_event_table_column_params},
            ]
        )

    assert to_dict(op_struct.columns) == expected_columns + [order_size_column]
    assert to_dict(op_struct.aggregations) == expected_aggregations
    assert op_struct.row_index_lineage == ("groupby_1",)

    grp_op_struct = op_struct.to_group_operation_structure()
    assert to_dict(grp_op_struct.source_columns) == expected_columns + [order_id_source_data]
    assert to_dict(grp_op_struct.derived_columns) == [order_size_column]
    assert to_dict(grp_op_struct.aggregations) == expected_aggregations
    assert grp_op_struct.post_aggregation is None
    assert grp_op_struct.row_index_lineage == ("groupby_1",)
    assert op_struct.is_time_based is True


@pytest.mark.parametrize("keep_all_source_columns", [True, False])
def test_extract_operation__lookup_feature(
    global_graph,
    lookup_feature_node,
    dimension_table_input_node,
    keep_all_source_columns,
):
    """Test extract_operation_structure: lookup features"""
    op_struct = global_graph.extract_operation_structure(
        node=lookup_feature_node, keep_all_source_columns=keep_all_source_columns
    )
    common_data_params = extract_column_parameters(dimension_table_input_node)
    cust_id_column = {"name": "cust_id", "dtype": "INT", **common_data_params}
    cust_value_1_column = {"name": "cust_value_1", "dtype": "FLOAT", **common_data_params}
    cust_value_2_column = {"name": "cust_value_2", "dtype": "FLOAT", **common_data_params}
    if keep_all_source_columns:
        expected_columns = [cust_id_column, cust_value_1_column, cust_value_2_column]
    else:
        expected_columns = [cust_value_1_column, cust_value_2_column]
    expected_aggregations = [
        {
            "filter": False,
            "node_names": {"input_1", "project_2", "add_1", "alias_1", "project_1", "lookup_1"},
            "node_name": "alias_1",
            "name": "MY FEATURE",
            "transforms": ["add"],
            "columns": [
                {
                    "filter": False,
                    "node_names": {"input_1", "project_1", "lookup_1"},
                    "node_name": "lookup_1",
                    "name": "CUSTOMER ATTRIBUTE 1",
                    "method": None,
                    "keys": ["cust_id"],
                    "window": None,
                    "category": None,
                    "type": "aggregation",
                    "column": cust_value_1_column,
                    "aggregation_type": "lookup",
                    "dtype": "FLOAT",
                },
                {
                    "filter": False,
                    "node_names": {"input_1", "project_2", "lookup_1"},
                    "node_name": "lookup_1",
                    "name": "CUSTOMER ATTRIBUTE 2",
                    "method": None,
                    "keys": ["cust_id"],
                    "window": None,
                    "category": None,
                    "type": "aggregation",
                    "column": cust_value_2_column,
                    "aggregation_type": "lookup",
                    "dtype": "FLOAT",
                },
            ],
            "type": "post_aggregation",
            "dtype": "FLOAT",
        }
    ]
    assert to_dict(op_struct.columns) == expected_columns
    assert to_dict(op_struct.aggregations) == expected_aggregations
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "series"
    assert op_struct.row_index_lineage == ("lookup_1",)
    assert op_struct.is_time_based is False

    # check main input nodes
    primary_input_nodes = global_graph.get_primary_input_nodes(node_name=lookup_feature_node.name)
    assert primary_input_nodes == [dimension_table_input_node]


@pytest.mark.parametrize("keep_all_source_columns", [True, False])
def test_extract_operation__event_lookup_feature(
    global_graph,
    event_lookup_feature_node,
    event_table_input_node,
    keep_all_source_columns,
):
    """Test extract_operation_structure: event lookup features"""
    op_struct = global_graph.extract_operation_structure(
        node=event_lookup_feature_node, keep_all_source_columns=keep_all_source_columns
    )
    common_data_params = extract_column_parameters(event_table_input_node)
    ts_col = {"name": "ts", "dtype": "TIMESTAMP", **common_data_params}
    order_id_col = {"name": "order_id", "dtype": "INT", **common_data_params}
    order_method_col = {"name": "order_method", "dtype": "VARCHAR", **common_data_params}
    if keep_all_source_columns:
        expected_columns = [ts_col, order_id_col, order_method_col]
    else:
        expected_columns = [ts_col, order_method_col]
    expected_aggregations = [
        {
            "name": "Order Method",
            "dtype": "VARCHAR",
            "filter": False,
            "node_names": {"lookup_1", "input_1", "project_1"},
            "node_name": "lookup_1",
            "method": None,
            "keys": ["order_id"],
            "window": None,
            "category": None,
            "type": "aggregation",
            "column": order_method_col,
            "aggregation_type": "lookup",
        }
    ]
    assert to_dict(op_struct.columns) == expected_columns
    assert to_dict(op_struct.aggregations) == expected_aggregations
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "series"
    assert op_struct.row_index_lineage == ("lookup_1",)
    assert op_struct.is_time_based is True

    # check main input nodes
    primary_input_nodes = global_graph.get_primary_input_nodes(
        node_name=event_lookup_feature_node.name
    )
    assert primary_input_nodes == [event_table_input_node]


@pytest.mark.parametrize("keep_all_source_columns", [True, False])
def test_extract_operation__scd_lookup_feature(
    global_graph,
    scd_lookup_feature_node,
    scd_table_input_node,
    keep_all_source_columns,
):
    """Test extract_operation_structure: SCD lookup features"""
    op_struct = global_graph.extract_operation_structure(
        node=scd_lookup_feature_node, keep_all_source_columns=keep_all_source_columns
    )
    common_data_params = extract_column_parameters(scd_table_input_node)
    cust_id_col = {"name": "cust_id", "dtype": "INT", **common_data_params}
    membership_status_col = {"name": "membership_status", "dtype": "VARCHAR", **common_data_params}
    if keep_all_source_columns:
        expected_columns = [cust_id_col, membership_status_col]
    else:
        expected_columns = [membership_status_col]
    expected_aggregations = [
        {
            "name": "Current Membership Status",
            "dtype": "VARCHAR",
            "filter": False,
            "node_names": {"lookup_1", "project_1", "input_1"},
            "node_name": "lookup_1",
            "method": None,
            "keys": ["cust_id"],
            "window": None,
            "category": None,
            "type": "aggregation",
            "column": membership_status_col,
            "aggregation_type": "lookup",
        }
    ]
    assert to_dict(op_struct.columns) == expected_columns
    assert to_dict(op_struct.aggregations) == expected_aggregations
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "series"
    assert op_struct.row_index_lineage == ("lookup_1",)
    assert op_struct.is_time_based is True

    # check main input nodes
    primary_input_nodes = global_graph.get_primary_input_nodes(
        node_name=scd_lookup_feature_node.name
    )
    assert primary_input_nodes == [scd_table_input_node]


@pytest.mark.parametrize("keep_all_source_columns", [True, False])
def test_extract_operation__aggregate_asat_feature(
    global_graph,
    aggregate_asat_feature_node,
    scd_table_input_node,
    keep_all_source_columns,
):
    """Test extract_operation_structure: features derived from aggregate_asat"""
    op_struct = global_graph.extract_operation_structure(
        node=aggregate_asat_feature_node, keep_all_source_columns=keep_all_source_columns
    )
    common_data_params = extract_column_parameters(scd_table_input_node)
    expected_columns = [
        {"name": "effective_ts", "dtype": "TIMESTAMP", **common_data_params},
        {"name": "cust_id", "dtype": "INT", **common_data_params},
    ]
    if keep_all_source_columns:
        expected_columns.append(
            {"name": "membership_status", "dtype": "VARCHAR", **common_data_params}
        )

    expected_aggregations = [
        {
            "name": "asat_feature",
            "dtype": "FLOAT",
            "filter": False,
            "node_names": {"input_1", "project_1", "aggregate_as_at_1"},
            "node_name": "aggregate_as_at_1",
            "method": "count",
            "keys": ["membership_status"],
            "window": None,
            "category": None,
            "type": "aggregation",
            "column": None,
            "aggregation_type": "aggregate_as_at",
        }
    ]

    assert to_dict(op_struct.columns) == expected_columns
    assert to_dict(op_struct.aggregations) == expected_aggregations
    assert op_struct.output_category == "feature"
    assert op_struct.output_type == "series"
    assert op_struct.row_index_lineage == ("aggregate_as_at_1",)
    assert op_struct.is_time_based is True

    # check main input nodes
    primary_input_nodes = global_graph.get_primary_input_nodes(
        node_name=aggregate_asat_feature_node.name
    )
    assert primary_input_nodes == [scd_table_input_node]


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
    op_struct = global_graph.extract_operation_structure(
        node=add_node, keep_all_source_columns=True
    )
    expected_derived_columns = {
        "name": None,
        "columns": [
            {"name": "a", "dtype": "FLOAT", **extract_column_parameters(input_node, {"project_1"})}
        ],
        "transforms": ["add(value=10)"],
        "filter": False,
        "type": "derived",
        "node_names": {"input_1", "project_1", "add_1"},
        "node_name": "add_1",
        "dtype": "FLOAT",
    }
    assert to_dict(op_struct.columns) == [expected_derived_columns]
    alias_node = global_graph.add_operation(
        node_type=NodeType.ALIAS,
        node_params={"name": "some_value"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[add_node],
    )
    op_struct = global_graph.extract_operation_structure(
        node=alias_node, keep_all_source_columns=True
    )
    assert to_dict(op_struct.columns) == [
        {
            **expected_derived_columns,
            "name": "some_value",
            "node_names": {"input_1", "project_1", "add_1", "alias_1"},
            "node_name": "alias_1",
        }
    ]
    assert op_struct.row_index_lineage == ("input_1",)


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

    op_struct = graph.extract_operation_structure(node=new_ts.node, keep_all_source_columns=True)
    expected_new_ts = {
        "name": "NEW_TIMESTAMP",
        "columns": [
            {
                "name": "TIMESTAMP_VALUE",
                "dtype": "TIMESTAMP",
                **extract_column_parameters(input_node, {"project_1", "project_3"}),
            }
        ],
        "transforms": ["date_diff", "date_add"],
        "type": "derived",
        "filter": False,
        "node_names": {
            "project_3",
            "assign_1",
            "project_1",
            "assign_3",
            "input_1",
            "project_2",
            "date_diff_1",
            "project_4",
            "date_add_1",
        },
        "node_name": "assign_3",
        "dtype": "TIMESTAMP",
    }
    assert to_dict(op_struct.columns) == [expected_new_ts]
    assert op_struct.row_index_lineage == ("input_1",)

    # assign_2 is not included in the lineage as `dataframe["diff"] = 123` does not affect
    # final value of NEW_TIMESTAMP column
    assert graph.get_node_by_name("assign_1").parameters == {"name": "diff", "value": None}
    assert graph.get_node_by_name("assign_3").parameters == {"name": "NEW_TIMESTAMP", "value": None}
    # check on constant value assignment
    op_struct = graph.extract_operation_structure(
        node=dataframe["diff"].node, keep_all_source_columns=True
    )
    assert to_dict(op_struct.columns) == [
        {
            "name": "diff",
            "columns": [],
            "transforms": [],
            "type": "derived",
            "filter": False,
            "node_names": {"assign_2", "project_5"},
            "node_name": "assign_2",
            "dtype": "INT",
        }
    ]
    assert graph.get_node_by_name("assign_2").parameters == {"name": "diff", "value": 123}
    assert op_struct.row_index_lineage == ("input_1",)

    # check frame
    op_struct = graph.extract_operation_structure(node=dataframe.node, keep_all_source_columns=True)
    expected_new_ts["node_names"].remove("project_4")
    assert to_dict(op_struct.columns) == [
        {"name": "CUST_ID", "dtype": "INT", **common_data_column_params},
        {"name": "PRODUCT_ACTION", "dtype": "VARCHAR", **common_data_column_params},
        {"name": "VALUE", "dtype": "FLOAT", **common_data_column_params},
        {"name": "MASK", "dtype": "BOOL", **common_data_column_params},
        {"name": "TIMESTAMP_VALUE", "dtype": "TIMESTAMP", **common_data_column_params},
        {
            "name": "diff",
            "dtype": "INT",
            "columns": [],
            "transforms": [],
            "type": "derived",
            "filter": False,
            "node_names": {"assign_2"},
            "node_name": "assign_2",
        },
        expected_new_ts,
    ]
    assert op_struct.row_index_lineage == ("input_1",)


def test_extract_operation__complicated_assignment_case_2(dataframe):
    """Test node names value tracks column lineage properly"""
    dataframe["diff"] = dataframe["VALUE"] - dataframe["CUST_ID"]
    dataframe["diff"] = dataframe["diff"] + dataframe["CUST_ID"]
    dataframe["another_diff"] = dataframe["diff"]
    dataframe["diff"] = dataframe["another_diff"]

    # check extract operation structure
    graph = dataframe.graph
    input_node = graph.get_node_by_name("input_1")
    op_struct = graph.extract_operation_structure(
        node=dataframe["diff"].node, keep_all_source_columns=True
    )
    assert to_dict(op_struct) == {
        "aggregations": [],
        "columns": [
            {
                "columns": [
                    {
                        "name": "VALUE",
                        "dtype": "FLOAT",
                        **extract_column_parameters(input_node, {"project_1", "input_1"}),
                    },
                    {
                        "name": "CUST_ID",
                        "dtype": "INT",
                        **extract_column_parameters(
                            input_node, {"project_2", "input_1", "project_4"}
                        ),
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
                    "project_7",
                },
                "node_name": "assign_4",
                "transforms": ["sub", "add"],
                "type": "derived",
                "dtype": "FLOAT",
            }
        ],
        "output_category": "view",
        "output_type": "series",
        "row_index_lineage": ("input_1",),
        "is_time_based": False,
    }


@pytest.mark.parametrize("keep_all_source_columns", [True, False])
def test_extract_operation_structure__groupby_on_event_timestamp_columns(
    query_graph_and_assign_node, groupby_node_params, keep_all_source_columns
):
    """Test extract operation structure with groupby on event timestamp column"""
    graph, assign_node = query_graph_and_assign_node
    groupby_node_params["parent"] = groupby_node_params["timestamp"]
    groupby_node_params["agg_func"] = "latest"
    groupby_node = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params=groupby_node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    op_struct = graph.extract_operation_structure(
        node=groupby_node, keep_all_source_columns=keep_all_source_columns
    )
    common_agg_params = {
        "aggregation_type": "groupby",
        "category": None,
        "column": {
            "dtype": "TIMESTAMP",
            "filter": False,
            "name": "ts",
            "node_name": "input_1",
            "node_names": {"input_1"},
            "table_id": None,
            "table_type": "event_table",
            "type": "source",
        },
        "dtype": "TIMESTAMP",
        "filter": False,
        "keys": ["cust_id"],
        "method": "latest",
        "node_name": "groupby_1",
        "node_names": {"groupby_1", "input_1"},
        "type": "aggregation",
    }
    common_column_params = {
        "filter": False,
        "node_name": "input_1",
        "node_names": {"input_1"},
        "table_id": None,
        "table_type": "event_table",
        "type": "source",
    }
    ts_col = {"name": "ts", "dtype": "TIMESTAMP", **common_column_params}
    expected_columns = [ts_col]
    if keep_all_source_columns:
        cust_id_col = {"name": "cust_id", "dtype": "INT", **common_column_params}
        expected_columns.append(cust_id_col)

    assert to_dict(op_struct.columns) == expected_columns
    assert to_dict(op_struct.aggregations) == [
        {"name": "a_2h_average", "window": "2h", **common_agg_params},
        {"name": "a_48h_average", "window": "48h", **common_agg_params},
    ]


def test_extract_operation_structure__graph_node_row_index_lineage(
    query_graph_with_cleaning_ops_graph_node,
):
    """Test row index lineage of the graph (cleaning type) node's operation structure"""
    query_graph, graph_node = query_graph_with_cleaning_ops_graph_node
    op_struct = query_graph.extract_operation_structure(
        node=graph_node, keep_all_source_columns=True
    )

    # check columns & aggregations
    common_params = {
        "node_names": {"input_1"},
        "node_name": "input_1",
        "filter": False,
        "table_id": None,
        "table_type": "event_table",
        "type": "source",
    }
    assert to_dict(op_struct.columns) == [
        {"name": "ts", "dtype": "TIMESTAMP", **common_params},
        {"name": "cust_id", "dtype": "INT", **common_params},
        {"name": "b", "dtype": "FLOAT", **common_params},
        {
            "name": "a",
            "dtype": "FLOAT",
            "filter": False,
            "node_names": {"input_1", "graph_1"},
            "node_name": "graph_1",
            "transforms": [],
            "columns": [
                {
                    "name": "a",
                    "dtype": "FLOAT",
                    "node_names": {"input_1", "graph_1"},
                    "node_name": "graph_1",
                    "filter": False,
                    "table_id": None,
                    "table_type": "event_table",
                    "type": "source",
                }
            ],
            "type": "derived",
        },
    ]
    assert op_struct.aggregations == []

    # make sure the row index lineage points to non-nested nodes
    assert op_struct.row_index_lineage == ("input_1",)


def test_track_changes_operation_structure(global_graph, scd_table_input_node):
    """Test track changes operation structure"""
    track_changes_node = global_graph.add_operation(
        node_type=NodeType.TRACK_CHANGES,
        node_params={
            "natural_key_column": "cust_id",
            "effective_timestamp_column": "effective_ts",
            "tracked_column": "membership_status",
            "previous_tracked_column_name": "previous_membership_status",
            "new_tracked_column_name": "new_membership_status",
            "previous_valid_from_column_name": "previous_valid_from",
            "new_valid_from_column_name": "new_valid_from",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[scd_table_input_node],
    )

    op_struct = global_graph.extract_operation_structure(
        node=track_changes_node, keep_all_source_columns=True
    )
    common_source_column_params = {
        "filter": False,
        "node_name": "input_1",
        "node_names": {"input_1"},
        "table_id": None,
        "table_type": "scd_table",
        "type": "source",
    }
    track_changes_params = {
        "columns": [
            {"name": "effective_ts", "dtype": "TIMESTAMP", **common_source_column_params},
            {"name": "membership_status", "dtype": "VARCHAR", **common_source_column_params},
        ],
        "filter": False,
        "node_name": "track_changes_1",
        "node_names": {"input_1", "track_changes_1"},
        "transforms": ["track_changes"],
        "type": "derived",
    }
    expected_columns = [
        {"name": "cust_id", "dtype": "INT", **common_source_column_params},
        {"name": "previous_membership_status", "dtype": "VARCHAR", **track_changes_params},
        {"name": "new_membership_status", "dtype": "VARCHAR", **track_changes_params},
        {"name": "previous_valid_from", "dtype": "TIMESTAMP", **track_changes_params},
        {"name": "new_valid_from", "dtype": "TIMESTAMP", **track_changes_params},
    ]
    assert to_dict(op_struct.columns) == expected_columns
    assert op_struct.aggregations == []
    assert op_struct.output_type == NodeOutputType.FRAME
    assert op_struct.output_category == NodeOutputCategory.VIEW


@pytest.mark.parametrize("keep_all_source_columns", [True, False])
def test_request_column_operation_structure(
    global_graph, time_since_last_event_feature_node, keep_all_source_columns
):
    """Test request column operation structure"""
    op_struct = global_graph.extract_operation_structure(
        node=time_since_last_event_feature_node, keep_all_source_columns=keep_all_source_columns
    )
    common_source_column_params = {
        "filter": False,
        "node_names": {"input_1"},
        "node_name": "input_1",
        "table_id": None,
        "table_type": "event_table",
        "type": "source",
    }
    source_column = {"name": "ts", "dtype": "TIMESTAMP", **common_source_column_params}
    expected_columns = [source_column]
    if keep_all_source_columns:
        cust_id_column = {"name": "cust_id", "dtype": "INT", **common_source_column_params}
        expected_columns.append(cust_id_column)
    expected_aggregation_columns = [
        {
            "name": "latest_event_timestamp_90d",
            "dtype": "TIMESTAMP",
            "filter": False,
            "node_names": {"input_1", "groupby_1", "project_1"},
            "node_name": "groupby_1",
            "method": "latest",
            "keys": ["cust_id"],
            "window": "90d",
            "category": None,
            "type": "aggregation",
            "column": source_column,
            "aggregation_type": "groupby",
        },
        {
            "name": "POINT_IN_TIME",
            "dtype": "TIMESTAMP",
            "filter": False,
            "node_names": {"request_column_1"},
            "node_name": "request_column_1",
            "method": None,
            "keys": [],
            "window": None,
            "category": None,
            "type": "aggregation",
            "column": None,
            "aggregation_type": "request_column",
        },
    ]
    expected_aggregations = [
        {
            "columns": expected_aggregation_columns,
            "filter": False,
            "name": "time_since_last_event",
            "node_names": {
                "groupby_1",
                "alias_1",
                "input_1",
                "date_diff_1",
                "project_1",
                "request_column_1",
            },
            "node_name": "alias_1",
            "transforms": ["date_diff"],
            "type": "post_aggregation",
            "dtype": "TIMEDELTA",
        }
    ]
    assert to_dict(op_struct.aggregations) == expected_aggregations
    assert to_dict(op_struct.columns) == expected_columns
    assert op_struct.output_type == NodeOutputType.SERIES
    assert op_struct.output_category == NodeOutputCategory.FEATURE
