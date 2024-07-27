"""
Module for distance related operations
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
def haversine(
    lat_series_1: Series, lon_series_1: Series, lat_series_2: Series, lon_series_2: Series
) -> Series:
    """
    Construct a Series that contains the haversince distances between two points.

    Parameters
    ----------
    lat_series_1 : Series
        Series representing the latitude of the first point
    lon_series_1 : Series
        Series representing the longitude of the first point
    lat_series_2 : Series
        Series representing the latitude of the second point
    lon_series_2 : Series
        Series representing the longitude of the second point

    Returns
    -------
    Series

    Examples
    --------
    >>> event_view = catalog.get_view("EVENT_VIEW")  # doctest: +SKIP
    >>> event_view["HAVERSINE_DIST"] = fb.haversine(  # doctest: +SKIP
    ...     event_view["LAT_1"], event_view["LON_2"], event_view["LAT_2"], event_view["LON_2"]
    ... )
    """
    _validate_series([lat_series_1, lon_series_1, lat_series_2, lon_series_2])

    node = lat_series_1.graph.add_operation(
        node_type=NodeType.HAVERSINE,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[lat_series_1.node, lon_series_1.node, lat_series_2.node, lon_series_2.node],
    )
    return type(lat_series_1)(
        feature_store=lat_series_1.feature_store,
        tabular_source=lat_series_1.tabular_source,
        node_name=node.name,
        name=None,
        dtype=DBVarType.FLOAT,
    )
