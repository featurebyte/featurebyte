"""
Module for Timedelta related functions
"""
from __future__ import annotations

from typeguard import typechecked

from featurebyte.common.typing import TimedeltaSupportedUnitType
from featurebyte.core.series import Series
from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType


@typechecked
def to_timedelta(series: Series, unit: TimedeltaSupportedUnitType) -> Series:
    """
    Construct a timedelta Series that can be used to increment a datetime Series.

    Parameters
    ----------
    series : Series
        Series representing the amount of time unit
    unit : TimedeltaSupportedUnitType
        A supported unit in str

    Returns
    -------
    Series

    Raises
    ------
    ValueError
        if input Series is not of INT type

    Examples
    --------
    >>> items_view = catalog.get_view("INVOICEITEMS")
    >>> items_view["DAYS_SINCE_LAST_INVOICE"] = to_timedelta(  # doctest: +SKIP
    ...   items_view["DAYS_SINCE_LAST_INVOICE"], "days"
    ... )
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
