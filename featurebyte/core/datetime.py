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
    """
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
