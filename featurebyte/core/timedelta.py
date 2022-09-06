"""
Module for Timedelta related functions
"""
from typing import Literal

from typeguard import typechecked

from featurebyte.core.series import Series
from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType

SupportedUnit = Literal[
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
    "nanosecond",
]


@typechecked
def to_timedelta(series: Series, unit: SupportedUnit) -> Series:
    """Construct a timedelta Series that can be used to increment a datetime Series

    Parameters
    ----------
    series : Series
        Series representing the amount of time unit
    unit : SupportedUnit
        A supported unit in str

    Returns
    -------
    Series

    Raises
    ------
    ValueError
        if input Series is not of INT type
    """
    if series.dtype != DBVarType.INT:
        raise ValueError(f"to_timedelta only supports INT type series; got {series.dtype}")

    timedelta_series = series_unary_operation(
        input_series=series,
        node_type=NodeType.TIMEDELTA,
        output_var_type=DBVarType.TIMEDELTA,
        node_params={"unit": unit},
        **series.unary_op_series_params(),
    )

    return timedelta_series
