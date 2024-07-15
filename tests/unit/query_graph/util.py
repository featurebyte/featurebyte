from dataclasses import asdict

import numpy as np
import pandas as pd
import scipy as sp

from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    OperationStructure,
    PostAggregationColumn,
    SourceDataColumn,
)


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
    if isinstance(obj, (SourceDataColumn, DerivedDataColumn, AggregationColumn, PostAggregationColumn)):
        return to_dict(asdict(obj), exclude=exclude, include=include)
    if hasattr(obj, "dict"):
        return to_dict(obj.dict(), exclude=exclude, include=include)
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
    if udf_stats:
        for stat in udf_stats:
            if isinstance(stat, tuple):
                stat = f"{stat[0]} = {stat[1]}"
            exec(stat, locals)

    out_vals = []
    input_param_names = list(input_map.keys())
    for input_vals in zip(*input_map.values()):
        for param_name, param_val in zip(input_param_names, input_vals):
            locals[param_name] = param_val
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
    pd.testing.assert_series_equal(out_odfv, out_udf)

    # check the expected output
    if expected_output is not None:
        pd.testing.assert_series_equal(out_odfv, expected_output)

    # check when all the values are NaN
    null_input_map = {}
    for key in input_map:
        null_input_map[key] = pd.Series([np.nan, None])

    # check the odfv expression can be evaluated
    out_odfv_null = _get_odfv_output(input_map=null_input_map, odfv_expr=odfv_expr, odfv_stats=odfv_stats)

    # check the udf expression can be evaluated
    out_udf_null = _get_udf_output(input_map=null_input_map, udf_expr=udf_expr, udf_stats=udf_stats)

    # check the consistency between two expressions
    pd.testing.assert_series_equal(out_odfv_null, out_udf_null, check_dtype=False)
