"""
util.py contains common functions used across different classes
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType

if TYPE_CHECKING:
    from featurebyte.core.series import Series

    SeriesT = TypeVar("SeriesT", bound=Series)


def series_unary_operation(
    input_series: SeriesT,
    node_type: NodeType,
    output_var_type: DBVarType,
    node_params: dict[str, Any],
    **kwargs: Any,
) -> SeriesT:
    """
    Apply an operation on the Series itself and return another Series

    Parameters
    ----------
    input_series : Series
        Series like input object
    node_type : NodeType
        Output node type
    output_var_type : DBVarType
        Output variable type
    node_params : dict[str, Any]
        Node parameters,
    kwargs : Any
        Other series parameters

    Returns
    -------
    Series
    """
    node = input_series.graph.add_operation(
        node_type=node_type,
        node_params=node_params,
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_series.node],
    )
    return type(input_series)(
        feature_store=input_series.feature_store,
        tabular_source=input_series.tabular_source,
        node=node,
        name=None,
        var_type=output_var_type,
        row_index_lineage=input_series.row_index_lineage,
        **kwargs,
    )
