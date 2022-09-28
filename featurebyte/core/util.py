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
    input_series : SeriesT
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
    SeriesT
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
        node_name=node.name,
        name=None,
        dtype=output_var_type,
        row_index_lineage=input_series.row_index_lineage,
        **kwargs,
    )


def series_binary_operation(
    input_series: SeriesT,
    other: int | float | str | bool | SeriesT,
    node_type: NodeType,
    output_var_type: DBVarType,
    right_op: bool = False,
    additional_node_params: dict[str, Any] | None = None,
    **kwargs: Any,
) -> SeriesT:
    """
    Apply binary operation between a Series and another object

    Parameters
    ----------
    input_series : SeriesT
        Series like input object
    other: int | float | str | bool | SeriesT
        right value of the binary operator
    node_type: NodeType
        binary operator node type
    output_var_type: DBVarType
        output of the variable type
    right_op: bool
        whether the binary operation is from right object or not
    additional_node_params: dict[str, Any] | None
        additional parameters to include as node parameters
    kwargs : Any
        Other series parameters

    Returns
    -------
    SeriesT
    """
    node_params: dict[str, Any] = {"right_op": right_op} if right_op else {}
    if additional_node_params is not None:
        node_params.update(additional_node_params)
    if isinstance(other, type(input_series)):
        node = input_series.graph.add_operation(
            node_type=node_type,
            node_params=node_params,
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_series.node, other.node],
        )
        return type(input_series)(
            feature_store=input_series.feature_store,
            tabular_source=input_series.tabular_source,
            node_name=node.name,
            name=None,
            dtype=output_var_type,
            row_index_lineage=input_series.row_index_lineage,
            **kwargs,
        )
    node_params["value"] = other
    node = input_series.graph.add_operation(
        node_type=node_type,
        node_params=node_params,
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_series.node],
    )
    return type(input_series)(
        feature_store=input_series.feature_store,
        tabular_source=input_series.tabular_source,
        node_name=node.name,
        name=None,
        dtype=output_var_type,
        row_index_lineage=input_series.row_index_lineage,
        **kwargs,
    )
