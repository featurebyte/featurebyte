"""
util.py contains common functions used across different classes
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, TypeVar

from featurebyte.enum import DBVarType
from featurebyte.models.base import PydanticObjectId
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
        **kwargs,
    )


def _validate_entity_ids(entity_ids: List[PydanticObjectId]) -> None:
    """
    Helper function to do some simple validation on entity IDs

    Parameters
    ----------
    entity_ids: List[PydanticObjectId]
        entity IDs

    Raises
    ------
    ValueError
        raised when there are none, or multiple, entity IDs
    """
    if len(entity_ids) != 1:
        raise ValueError(f"no, or multiple, entity IDs found for the feature - {entity_ids}")


def _validate_entity(input_series: SeriesT, other_series: SeriesT) -> None:
    """
    Validates that the entities are
    - either the same, or
    - have a parent child relationship

    Parameters
    ----------
    input_series: SeriesT
        series
    other_series: SeriesT
        series

    Raises
    ------
    ValueError
        raised when the series are not related entities
    """
    input_entity_ids = input_series.entity_ids
    other_entity_ids = other_series.entity_ids
    _validate_entity_ids(input_entity_ids)
    _validate_entity_ids(other_entity_ids)

    # Check if entities are the same
    input_entity_id = input_entity_ids[0]
    other_entity_id = other_entity_ids[0]
    if input_entity_id == other_entity_id:
        return

    # Check if entities have a parent child relationship
    # TODO:

    raise ValueError("entities are not the same type, and do not have a parent-child relationship")


def _is_from_same_data(input_series: SeriesT, other_series: SeriesT) -> bool:
    """
    Checks if the two series are from the same item data

    Parameters
    ----------
    input_series: SeriesT
        series
    other_series: SeriesT
        series

    Returns
    -------
    bool
        True, if both are from the same item data, False otherwise
    """
    # TODO: do we need to verify if it's item, or event data
    input_table_name = input_series.tabular_source.table_details.table_name
    other_table_name = other_series.tabular_source.table_details.table_name
    return input_table_name == other_table_name


def _is_series_a_lookup_feature(series: SeriesT) -> bool:
    """
    Checks if a series is a lookup feature. Does this by looking through all the nodes
    in the related graph to see if there's a lookup node.

    Parameters
    ----------
    series: SeriesT
        series

    Returns
    -------
    bool
        True, if the series is a lookup feature, False otherwise
    """
    return any(node.type == NodeType.LOOKUP for node in series.graph.dict()["nodes"])


def _both_are_lookup_features(input_series: SeriesT, other_series: SeriesT) -> bool:
    """
    Checks if the two series are lookup features

    Parameters
    ----------
    input_series: SeriesT
        series
    other_series: SeriesT
        series

    Returns
    -------
    bool
        True, if both are lookup features, False otherwise
    """
    return _is_series_a_lookup_feature(input_series) and _is_series_a_lookup_feature(other_series)


def _item_data_and_event_data_are_related(input_series: SeriesT, other_series: SeriesT) -> bool:
    """
    Checks if the item and event data are related.

    Parameters
    ----------
    input_series: SeriesT
        series
    other_series: SeriesT
        series

    Returns
    -------
    bool
        True, if item and event data are related, False otherwise
    """
    _, _ = input_series, other_series
    # TODO:
    return False


def _validate_feature_type(input_series: SeriesT, other_series: SeriesT) -> None:
    """
    Validates that the features are
    - Lookup features (Create Lookup Features)
    - from the same Item Data
    - from the same Event Data
    - or from Event Data and Item Data that are related

    Parameters
    ----------
    input_series: SeriesT
        series
    other_series: SeriesT
        series

    Raises
    ------
    ValueError
        raised when a series fails validation
    """
    if (
        _both_are_lookup_features(input_series, other_series)
        or _is_from_same_data(input_series, other_series)
        or _item_data_and_event_data_are_related(input_series, other_series)
    ):
        return
    raise ValueError("features are not of the right type")


def validate_series(input_series: SeriesT, other_series: SeriesT) -> None:
    """
    Validates series

    entity -> how do we find this?
    lookup feature -> can probably find inside the graph?
    related event/item data -> this can be found via tabular source i'm guessing

    Parameters
    ----------
    input_series: SeriesT
        series
    other_series: SeriesT
        series

    Raises
    ------
    ValueError
        raised when a series fails validation
    """
    _validate_entity(input_series, other_series)
    _validate_feature_type(input_series, other_series)


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
        **kwargs,
    )
