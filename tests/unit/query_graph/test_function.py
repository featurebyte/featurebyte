"""
This module contains generic function related node classes
"""
from featurebyte.enum import AggFunc, DBVarType, TableDataType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    NodeOutputCategory,
    PostAggregationColumn,
    SourceDataColumn,
)


def test_generic_function__view_type(global_graph, input_node):
    """Test adding generic function node to query graph (View)"""
    proj_a = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    gfunc = global_graph.add_operation(
        node_type=NodeType.GENERIC_FUNCTION,
        node_params={
            "function_name": "my_func",
            "function_args": [
                {"column_name": "a", "type": "column"},
                {"value": 1, "type": "value"},
            ],
            "output_dtype": DBVarType.FLOAT,
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a],
    )

    # check node methods & attributes
    assert gfunc.max_input_count == 1
    assert gfunc.derive_var_type(inputs=[]) == DBVarType.FLOAT

    # check output operation structure
    op_struct = global_graph.extract_operation_structure(node=gfunc)
    assert op_struct.is_time_based is False
    assert op_struct.output_category == NodeOutputCategory.VIEW
    assert op_struct.output_type == NodeOutputType.SERIES
    assert op_struct.columns == [
        DerivedDataColumn(
            name=None,
            dtype=DBVarType.FLOAT,
            filter=False,
            node_names={"input_1", "project_1", "generic_function_1"},
            node_name="generic_function_1",
            transforms=["my_func"],
            columns=[
                SourceDataColumn(
                    name="a",
                    dtype=DBVarType.FLOAT,
                    filter=False,
                    node_names={"input_1", "project_1"},
                    node_name="input_1",
                    table_id=None,
                    table_type=TableDataType.EVENT_TABLE,
                )
            ],
        ),
    ]
    assert op_struct.aggregations == []


def test_generic_function__feature_type(global_graph, query_graph_with_groupby_and_feature_nodes):
    """Test adding generic function node to query graph (Feature)"""
    graph, feature_proj, _ = query_graph_with_groupby_and_feature_nodes
    gfunc = graph.add_operation(
        node_type=NodeType.GENERIC_FUNCTION,
        node_params={
            "function_name": "my_func",
            "function_args": [
                {"column_name": "a", "type": "column"},
                {"value": 1, "type": "value"},
            ],
            "output_dtype": DBVarType.FLOAT,
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[feature_proj],
    )

    # check node methods & attributes
    assert gfunc.max_input_count == 1
    assert gfunc.derive_var_type(inputs=[]) == DBVarType.FLOAT

    # check output operation structure
    op_struct = graph.extract_operation_structure(node=gfunc)
    assert op_struct.is_time_based is True
    assert op_struct.output_category == NodeOutputCategory.FEATURE
    assert op_struct.output_type == NodeOutputType.SERIES
    assert op_struct.columns == [
        SourceDataColumn(
            name="a",
            dtype=DBVarType.FLOAT,
            filter=False,
            node_names={"input_1"},
            node_name="input_1",
            table_id=None,
            table_type=TableDataType.EVENT_TABLE,
        ),
    ]
    assert op_struct.aggregations == [
        PostAggregationColumn(
            name=None,
            dtype=DBVarType.FLOAT,
            filter=False,
            node_names={"input_1", "groupby_1", "project_3", "generic_function_1"},
            node_name="generic_function_1",
            transforms=["my_func"],
            columns=[
                AggregationColumn(
                    name="a_2h_average",
                    dtype=DBVarType.FLOAT,
                    filter=False,
                    node_names={"input_1", "groupby_1", "project_3"},
                    node_name="groupby_1",
                    method=AggFunc.AVG,
                    keys=["cust_id"],
                    window="2h",
                    category=None,
                    aggregation_type="groupby",
                    column=SourceDataColumn(
                        name="a",
                        dtype=DBVarType.FLOAT,
                        filter=False,
                        node_names={"input_1"},
                        node_name="input_1",
                        table_id=None,
                        table_type=TableDataType.EVENT_TABLE,
                    ),
                )
            ],
        )
    ]
