"""
Helper functions for vector aggregation
"""

from __future__ import annotations

from typing import Optional

from featurebyte.enum import AggFunc, DBVarType


def should_use_element_wise_vector_aggregation(
    agg_func: AggFunc, parent_dtype: Optional[DBVarType]
) -> bool:
    """
    Check if the aggregation requires element-wise vector aggregation

    Parameters
    ----------
    agg_func: AggFunc
        Aggregation function
    parent_dtype: Optional[DBVarType]
        Parent column data type

    Returns
    -------
    bool
    """
    return agg_func != AggFunc.LATEST and parent_dtype in DBVarType.array_types()
