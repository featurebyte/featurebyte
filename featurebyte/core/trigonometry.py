"""
Module for trigonometry related operations
"""

from typeguard import typechecked

from featurebyte.core.series import Series
from featurebyte.core.util import validate_numeric_series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


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
    validate_numeric_series([y, x])

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
