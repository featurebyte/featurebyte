"""
Vector aggregate validator
"""

from typing import List, Optional

from featurebyte.enum import AggFunc, DBVarType
from featurebyte.query_graph.model.column_info import ColumnInfo

VECTOR_AGGREGATE_SUPPORTED_FUNCTIONS = {AggFunc.MAX, AggFunc.AVG, AggFunc.SUM}


def validate_vector_aggregate_parameters(
    columns_info: List[ColumnInfo], value_column: Optional[str], method: Optional[str]
) -> None:
    """
    Validate vector aggregate parameters

    Parameters
    ----------
    columns_info: List[ColumnInfo]
        List of column info
    value_column: Optional[str]
        Column to be aggregated
    method: Optional[str]
        Aggregation method

    Raises
    ------
    ValueError
        If the method is not supported for vector aggregate operations
    """
    if value_column is None:
        return

    if method == AggFunc.LATEST:
        # Latest aggregation does not require element-wise aggregation
        return

    # Check whether the value_column's type is an array
    for info in columns_info:
        # If the type of the value_column is not an array, can return. If it is, break out of the loop and
        # check the method.
        if info.name == value_column and info.dtype not in DBVarType.array_types():
            return

    # If it's an array and requires element-wise aggregation, check whether the method is supported.
    # We currently only support MAX, AVG and SUM operations.
    if method not in VECTOR_AGGREGATE_SUPPORTED_FUNCTIONS:
        raise ValueError(f"Method {method} is not supported for vector aggregate operations.")
