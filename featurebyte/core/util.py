"""
util.py contains common functions used across different classes
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.scalar import get_value_parameter
from featurebyte.typing import AllSupportedValueTypes, Scalar, ScalarSequence

if TYPE_CHECKING:
    from featurebyte.core.series import FrozenSeries, FrozenSeriesT


def series_unary_operation(
    input_series: FrozenSeriesT,
    node_type: NodeType,
    output_var_type: DBVarType,
    node_params: dict[str, Any],
    **kwargs: Any,
) -> FrozenSeriesT:
    """
    Apply an operation on the Series itself and return another Series

    Parameters
    ----------
    input_series : FrozenSeriesT
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
    FrozenSeriesT
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
        **kwargs,
    )


class SeriesBinaryOperator:
    """
    Perform a binary operation between a Series, and another value.
    """

    def __init__(
        self,
        input_series: FrozenSeriesT,
        other: Scalar | FrozenSeries | ScalarSequence,
    ):
        self.input_series = input_series
        self.other = other

    def validate_inputs(self) -> None:
        """
        Validate the input series, and other parameter.

        Override this for any custom validation that is needed.
        """
        return

    def operate(
        self,
        node_type: NodeType,
        output_var_type: DBVarType,
        right_op: bool = False,
        additional_node_params: dict[str, Any] | None = None,
    ) -> FrozenSeriesT:  # type: ignore
        """
        Perform the series binary operation.

        Parameters
        ----------
        node_type: NodeType
            binary operator node type
        output_var_type: DBVarType
            output of the variable type
        right_op: bool
            whether the binary operation is from right object or not
        additional_node_params: dict[str, Any] | None
            additional parameters to include as node parameters

        Returns
        -------
        FrozenSeriesT
        """
        self.validate_inputs()
        return series_binary_operation(
            input_series=self.input_series,
            other=self.other,
            node_type=node_type,
            output_var_type=output_var_type,
            right_op=right_op,
            additional_node_params=additional_node_params,
        )


def construct_binary_op_series_output(
    input_series: FrozenSeries, other: FrozenSeries, node_name: str, output_var_type: DBVarType
) -> FrozenSeriesT:  # type: ignore
    """
    Construct the output series for binary operation between two series.

    Parameters
    ----------
    input_series : FrozenSeries
        Input series
    other : FrozenSeries
        Other series
    node_name : str
        Node name
    output_var_type : DBVarType
        Output variable type

    Returns
    -------
    FrozenSeriesT
    """
    if input_series.binary_op_output_class_priority <= other.binary_op_output_class_priority:
        output_type_series = input_series
    else:
        output_type_series = other
    return type(output_type_series)(  # type: ignore[return-value]
        feature_store=output_type_series.feature_store,
        tabular_source=output_type_series.tabular_source,
        node_name=node_name,
        name=None,
        dtype=output_var_type,
    )


def series_binary_operation(
    input_series: FrozenSeriesT,
    other: Scalar | FrozenSeries | ScalarSequence,
    node_type: NodeType,
    output_var_type: DBVarType,
    right_op: bool = False,
    additional_node_params: dict[str, Any] | None = None,
) -> FrozenSeriesT:
    """
    Apply binary operation between a Series and another object

    Parameters
    ----------
    input_series : FrozenSeriesT
        Series like input object
    other: Scalar | FrozenSeries | ScalarSequence
        right value of the binary operator
    node_type: NodeType
        binary operator node type
    output_var_type: DBVarType
        output of the variable type
    right_op: bool
        whether the binary operation is from right object or not
    additional_node_params: dict[str, Any] | None
        additional parameters to include as node parameters

    Returns
    -------
    FrozenSeriesT
    """

    from featurebyte.core.series import FrozenSeries

    node_params: dict[str, Any] = {"right_op": right_op} if right_op else {}
    if additional_node_params is not None:
        node_params.update(additional_node_params)
    if isinstance(other, FrozenSeries):
        node = input_series.graph.add_operation(
            node_type=node_type,
            node_params=node_params,
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_series.node, other.node],
        )
        return construct_binary_op_series_output(input_series, other, node.name, output_var_type)
    other = cast(AllSupportedValueTypes, other)
    node_params["value"] = get_value_parameter(other)
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
    )
