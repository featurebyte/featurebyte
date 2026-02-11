"""
Module for distance related operations
"""

from typeguard import typechecked

from featurebyte.core.series import Series
from featurebyte.core.util import validate_numeric_series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


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
    validate_numeric_series([lat_series_1, lon_series_1, lat_series_2, lon_series_2])

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
