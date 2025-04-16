from dataclasses import asdict
from unittest.mock import Mock

import numpy as np
import pandas as pd
import scipy as sp
from sqlglot import expressions

from featurebyte.enum import SourceType
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    OperationStructure,
    PostAggregationColumn,
    SourceDataColumn,
)
from featurebyte.query_graph.sql.aggregator.base import Aggregator
from featurebyte.query_graph.sql.ast.base import SQLNodeContext
from featurebyte.query_graph.sql.ast.generic import StrExpressionNode
from featurebyte.query_graph.sql.common import SQLType, construct_cte_sql
from featurebyte.query_graph.sql.source_info import SourceInfo


def to_dict(obj, exclude=None, include=None):
    """Convert object to dict form for more readable pytest assert reporting"""
    if isinstance(obj, OperationStructure):
        op_struct_dict = {
            "output_type": obj.output_type,
            "output_category": obj.output_category,
            "row_index_lineage": obj.row_index_lineage,
            "columns": obj.columns,
            "aggregations": obj.aggregations,
            "is_time_based": obj.is_time_based,
        }
        return to_dict(op_struct_dict, exclude=exclude, include=include)
    if isinstance(obj, list):
        return [to_dict(x) for x in obj]
    if isinstance(obj, dict):
        return {
            key: to_dict(value)
            for key, value in obj.items()
            if (exclude is None or key not in exclude) and (include is None or key in include)
        }
    if isinstance(
        obj, (SourceDataColumn, DerivedDataColumn, AggregationColumn, PostAggregationColumn)
    ):
        col_dict = to_dict(asdict(obj), exclude=exclude, include=include)
        if "dtype_info" in col_dict:
            col_dict["dtype"] = col_dict.pop("dtype_info")["dtype"]
        if isinstance(obj, AggregationColumn):
            if "column" in col_dict:
                col_dict["column"] = to_dict(obj.column)
        if isinstance(obj, (DerivedDataColumn, PostAggregationColumn)):
            if "columns" in col_dict:
                col_dict["columns"] = [to_dict(col) for col in obj.columns]
        return col_dict
    if hasattr(obj, "dict"):
        return to_dict(obj.model_dump(), exclude=exclude, include=include)
    return obj


def _get_odfv_output(input_map, odfv_expr, odfv_stats=None):
    # check the odfv expression can be evaluated
    locals = {"np": np, "pd": pd, "sp": sp, **input_map}
    if odfv_stats:
        for stat in odfv_stats:
            if isinstance(stat, tuple):
                stat = f"{stat[0]} = {stat[1]}"
            exec(stat, locals)

    out_odfv = eval(odfv_expr, locals)
    return out_odfv


def _get_udf_output(input_map, udf_expr, udf_stats=None):
    # check the udf expression can be evaluated
    locals = {"np": np, "pd": pd, "sp": sp}

    out_vals = []
    input_param_names = list(input_map.keys())
    for input_vals in zip(*input_map.values()):
        for param_name, param_val in zip(input_param_names, input_vals):
            locals[param_name] = param_val

        if udf_stats:
            for stat in udf_stats:
                if isinstance(stat, tuple):
                    stat = f"{stat[0]} = {stat[1]}"
                exec(stat, locals, locals)
        out_vals.append(eval(udf_expr, locals))
    out_udf = pd.Series(out_vals)
    return out_udf


def evaluate_and_compare_odfv_and_udf_results(
    input_map, odfv_expr, udf_expr, odfv_stats=None, udf_stats=None, expected_output=None
):
    """Evaluate the odfv & udf expressions & compare the results"""
    # check the odfv expression can be evaluated
    out_odfv = _get_odfv_output(input_map=input_map, odfv_expr=odfv_expr, odfv_stats=odfv_stats)

    # check the udf expression can be evaluated
    out_udf = _get_udf_output(input_map=input_map, udf_expr=udf_expr, udf_stats=udf_stats)

    # check the consistency between two expressions
    pd.testing.assert_series_equal(out_odfv, out_udf, check_dtype=False, check_names=False)

    # check the expected output
    if expected_output is not None:
        pd.testing.assert_series_equal(out_odfv, expected_output)

    # check when all the values are NaN
    null_input_map = {}
    for key in input_map:
        null_input_map[key] = pd.Series([np.nan, None])

    # check the odfv expression can be evaluated
    out_odfv_null = _get_odfv_output(
        input_map=null_input_map, odfv_expr=odfv_expr, odfv_stats=odfv_stats
    )

    # check the udf expression can be evaluated
    out_udf_null = _get_udf_output(input_map=null_input_map, udf_expr=udf_expr, udf_stats=udf_stats)

    # check the consistency between two expressions
    pd.testing.assert_series_equal(
        out_odfv_null, out_udf_null, check_dtype=False, check_names=False
    )


def get_combined_aggregation_expr_from_aggregator(
    aggregator: Aggregator, request_columns: list[str] | None = None
):
    """
    Get the combined aggregation expression from the aggregator
    """
    if request_columns is None:
        request_columns = ["cust_id"]
    result = aggregator.update_aggregation_table_expr(
        expressions.select("POINT_IN_TIME", *request_columns).from_("REQUEST_TABLE"),
        "POINT_IN_TIME",
        ["POINT_IN_TIME"] + request_columns,
        0,
    )
    result_expr = result.updated_table_expr
    select_with_ctes = construct_cte_sql(aggregator.get_common_table_expressions("REQUEST_TABLE"))
    result_expr.args["with"] = select_with_ctes.args["with"]
    return result_expr


def make_context(
    node_type=None, parameters=None, input_sql_nodes=None, sql_type=None, source_type=None
):
    """
    Helper function to create a SQLNodeContext with only arguments that matter in tests
    """
    if parameters is None:
        parameters = {}
    if sql_type is None:
        sql_type = SQLType.MATERIALIZE
    if source_type is None:
        source_type = SourceType.SNOWFLAKE
    source_info = SourceInfo(source_type=source_type, database_name="db", schema_name="public")
    mock_query_node = Mock(type=node_type)
    mock_query_node.parameters.model_dump.return_value = parameters
    mock_graph = Mock()
    context = SQLNodeContext(
        graph=mock_graph,
        query_node=mock_query_node,
        input_sql_nodes=input_sql_nodes,
        sql_type=sql_type,
        source_info=source_info,
        to_filter_scd_by_current_flag=False,
        event_table_timestamp_filter=None,
        aggregation_specs=None,
        on_demand_entity_filters=None,
    )
    return context


def make_str_expression_node(table_node, expr):
    """
    Helper function to create a StrExpressionNode used only in tests
    """
    return StrExpressionNode(make_context(), table_node=table_node, expr=expr)
