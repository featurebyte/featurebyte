"""
Module for datetime related operations
"""

from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


def to_timestamp_from_epoch(values: Series) -> Series:
    """
    Convert epoch seconds to timestamp values

    Parameters
    ----------
    values: Series
        Series containing epoch seconds

    Returns
    -------
    Series

    Raises
    ------
    ValueError
        if input is not numeric

    Examples
    --------
    Create a new column in the GROCERYINVOICE view that represents the timestamp from epoch seconds.

    >>> view = catalog.get_view("GROCERYINVOICE")
    >>> view["CONVERTED_TIMESTAMP"] = fb.to_timestamp_from_epoch(
    ...     view["EPOCH_SECOND"]
    ... )  # doctest: +SKIP
    """
    if not values.is_numeric:
        raise ValueError(
            f"to_timestamp_from_epoch requires input to be numeric, got {values.dtype}"
        )

    node = values.graph.add_operation(
        node_type=NodeType.TO_TIMESTAMP_FROM_EPOCH,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[values.node],
    )
    return type(values)(
        feature_store=values.feature_store,
        tabular_source=values.tabular_source,
        node_name=node.name,
        name=values.name,
        dtype=DBVarType.TIMESTAMP,
    )
