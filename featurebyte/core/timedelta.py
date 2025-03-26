"""
Module for Timedelta related functions
"""

from __future__ import annotations

from typeguard import typechecked

from featurebyte.core.series import Series
from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.typing import TimedeltaSupportedUnitType


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
    Create a new column in the INVOICEITEMS view that represents the number of days since the last invoice.

    >>> items_view = catalog.get_view("INVOICEITEMS")
    >>> items_view["TIMEDELTA_DAYS_SINCE_LAST_INVOICE"] = fb.to_timedelta(  # doctest: +SKIP
    ...     items_view["DAYS_SINCE_LAST_INVOICE"], "day"
    ... )

    Create a new column that is 10 minutes after the event.

    >>> items_view = catalog.get_view("INVOICEITEMS")
    >>> items_view["offset"] = 10
    >>> items_view["10_MINS_AFTER_EVENT"] = items_view["Timestamp"] + fb.to_timedelta(
    ...     items_view["offset"], unit="minute"
    ... )
    """
    if series.dtype != DBVarType.INT:
        raise ValueError(f"to_timedelta only supports INT type series; got {series.dtype}")

    timedelta_series = series_unary_operation(
        input_series=series,
        node_type=NodeType.TIMEDELTA,
        output_var_type=DBVarType.TIMEDELTA,
        node_params={"unit": unit},
    )

    return timedelta_series
