"""
Module for distance related operations
"""
from typeguard import typechecked

from featurebyte.core.series import Series
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

    Raises
    ------
    ValueError
        if any of the Series are not of FLOAT type

    Examples
    --------
    >>> event_view = catalog.get_view("EVENT_VIEW")  # doctest: +SKIP
    >>> event_view["HAVERSINE_DIST"] = fb.haversine(  # doctest: +SKIP
    ...   event_view["LAT_1"], event_view["LON_2"], event_view["LAT_2"], event_view["LON_2"]
    ... )
    """
    if (
        not lat_series_1.is_numeric
        or not lon_series_1.is_numeric
        or not lat_series_2.is_numeric
        or not lon_series_2.is_numeric
    ):
        raise ValueError(
            f"haversine only supports series of numeric types; got:\n"
            f"lat_series_1: {lat_series_1.dtype},\n"
            f"lon_series_1: {lon_series_1.dtype},\n"
            f"lat_series_2: {lat_series_2.dtype},\n"
            f"lon_series_2: {lon_series_2.dtype},\n"
        )

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
