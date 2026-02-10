"""
Module for trigonometry related operations
"""

from typing import Any, List, Optional

from typeguard import typechecked

from featurebyte import Feature, Target
from featurebyte.api.view import ViewColumn
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


def _validate_series(series_list: List[Series]) -> None:
    """
    Validate that:
    - all series are numeric
    - all series are of the same type (all ViewColumns, or Features, or Targets)

    Parameters
    ----------
    series_list: List[Series]
        List of series to validate

    Raises
    ------
    ValueError
        if any of the series are not numeric
    """
    # Check numeric
    for series in series_list:
        if not series.is_numeric:
            raise ValueError(
                f"Expected all series to be numeric, but got {series.dtype} for series {series.name}"
            )

    # Check all series are of the same type
    series_type_to_check: Optional[Any] = Series
    if isinstance(series_list[0], ViewColumn):
        series_type_to_check = ViewColumn
    elif isinstance(series_list[0], Feature):
        series_type_to_check = Feature
    elif isinstance(series_list[0], Target):
        series_type_to_check = Target

    if not series_type_to_check:
        raise ValueError(f"unknown series type found: {series_list[0].name}")

    for series in series_list:
        assert isinstance(series, series_type_to_check)


@typechecked
def atan2(y: Series, x: Series) -> Series:
    """
    Construct a Series that contains the two-argument arctangent (atan2) values.

    Returns the angle in radians between the positive x-axis of a plane and the point
    given by the coordinates (x, y) on it.

    Parameters
    ----------
    y : Series
        Series representing the y-coordinate
    x : Series
        Series representing the x-coordinate

    Returns
    -------
    Series

    Examples
    --------
    >>> event_view = catalog.get_view("EVENT_VIEW")  # doctest: +SKIP
    >>> event_view["ANGLE"] = fb.atan2(event_view["Y"], event_view["X"])  # doctest: +SKIP
    """
    _validate_series([y, x])

    node = y.graph.add_operation(
        node_type=NodeType.ATAN2,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[y.node, x.node],
    )
    return type(y)(
        feature_store=y.feature_store,
        tabular_source=y.tabular_source,
        node_name=node.name,
        name=None,
        dtype=DBVarType.FLOAT,
    )
