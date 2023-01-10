"""
Series validator module
"""
from __future__ import annotations

from typing import TYPE_CHECKING, List, Tuple, TypeVar, cast

from featurebyte.api.entity import Entity
from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.generic import InputNode
from featurebyte.query_graph.node.metadata.operation import DerivedDataColumn

if TYPE_CHECKING:
    from featurebyte.core.series import Series

    SeriesT = TypeVar("SeriesT", bound=Series)


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


def _is_parent_child(entity_a: Entity, entity_b: Entity) -> bool:
    """
    Helper function to determine if entity A is the parent of entity B.

    Parameters
    ----------
    entity_a: Entity
        entity
    entity_b: Entity
        entity

    Returns
    -------
    bool
        True if entity A is the parent of entity B.
    """
    entity_b_parent_ids = [entity.id for entity in entity_b.parents]
    return entity_a.id in entity_b_parent_ids


def validate_entities(
    input_entity_ids: List[PydanticObjectId], other_entity_ids: List[PydanticObjectId]
) -> None:
    """
    Validates that the entities are
    - either the same, or
    - have a parent child relationship

    Parameters
    ----------
    input_entity_ids: List[PydanticObjectId]
        input entity IDs
    other_entity_ids: List[PydanticObjectId]
        other entity IDs

    Raises
    ------
    ValueError
        raised when the series are not related entities
    """
    _validate_entity_ids(input_entity_ids)
    _validate_entity_ids(other_entity_ids)

    # Check if entities are the same
    input_entity_id = input_entity_ids[0]
    other_entity_id = other_entity_ids[0]
    if input_entity_id == other_entity_id:
        return

    # Check if entities have a parent child relationship
    input_entity = Entity.get_by_id(input_entity_id)
    other_entity = Entity.get_by_id(other_entity_id)
    if _is_parent_child(input_entity, other_entity) or _is_parent_child(other_entity, input_entity):
        return

    raise ValueError(
        f"entities {input_entity} and {other_entity} are not the same type, and do not have a parent-child relationship"
    )


def _series_data_type_and_tabular_id(
    input_series: SeriesT,
) -> Tuple[TableDataType, PydanticObjectId]:
    """
    Get table data type and tabular ID

    Parameters
    ----------
    input_series: SeriesT
        input series

    Returns
    -------
    Tuple[TableDataType, PydanticObjectId]
        table data type and tabular ID
    """
    operation_structure = input_series.graph.extract_operation_structure(input_series.node)
    # we only expect feature series to have a single column
    column_structure = operation_structure.columns[0]
    if isinstance(column_structure, DerivedDataColumn):
        column = column_structure.columns[0]
        tabular_data_id = column.tabular_data_id
        assert tabular_data_id is not None
        return column.tabular_data_type, tabular_data_id
    # column_structure is a SourceDataColumn
    tabular_data_id = column_structure.tabular_data_id
    assert tabular_data_id is not None
    return column_structure.tabular_data_type, tabular_data_id


def _are_series_both_of_type(
    type_a: TableDataType, type_b: TableDataType, target_type: TableDataType
) -> bool:
    """
    Helper function to determine if both types are of a particular target type

    Parameters
    ----------
    type_a: TableDataType
        type a
    type_b: TableDataType
        type b
    target_type: TableDataType
        target type

    Returns
    -------
    bool
        True if both the types are of the target type
    """
    return type_a == type_b and type_a == target_type


def _is_from_same_data(
    input_series_data_type: TableDataType,
    other_series_data_type: TableDataType,
    input_series_tabular_data_id: PydanticObjectId,
    other_series_tabular_data_id: PydanticObjectId,
) -> bool:
    """
    Helper function to determine if both series are from the same data.

    Parameters
    ----------
    input_series_data_type: TableDataType
        input series data type
    other_series_data_type: TableDataType
        other series data type
    input_series_tabular_data_id: PydanticObjectId
        input series tabular data id
    other_series_tabular_data_id: PydanticObjectId
        other series tabular data ID

    Returns
    -------
    bool
        True if both series are from the same data
    """
    if not _are_series_both_of_type(
        input_series_data_type, other_series_data_type, TableDataType.ITEM_DATA
    ) and not _are_series_both_of_type(
        input_series_data_type, other_series_data_type, TableDataType.EVENT_DATA
    ):
        return False
    return input_series_tabular_data_id == other_series_tabular_data_id


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
    input_is_lookup = NodeType.LOOKUP in input_series.node_types_lineage
    other_is_lookup = NodeType.LOOKUP in other_series.node_types_lineage
    return input_is_lookup and other_is_lookup


def _get_event_and_item_data_series(
    series_a_data_type: TableDataType, series_a: SeriesT, series_b: SeriesT
) -> Tuple[SeriesT, SeriesT]:
    """
    Helper function to determine which series belongs to the item data, and which is the event data.

    This assumes that there is definitely one item data, and event data series. Use the other helper function
    _is_one_item_and_one_event beforehand.

    Parameters
    ----------
    series_a_data_type: TableDataType
        series data type
    series_a: SeriesT
        series
    series_b: SeriesT
        series

    Returns
    -------
    Tuple[SeriesT, SeriesT]
        (item data series, event data series)
    """
    if series_a_data_type == TableDataType.ITEM_DATA:
        return series_a, series_b
    return series_b, series_a


def _is_one_item_and_one_event(
    series_a_node_type: TableDataType, series_b_node_type: TableDataType
) -> bool:
    """
    Helper function to determine if exactly one series is from an item data, and one is from an event data.

    Parameters
    ----------
    series_a_node_type: TableDataType
        series
    series_b_node_type: TableDataType
        series

    Returns
    -------
    bool
        True, if there's exactly one series that is from an item data, and one from an event data.
    """
    at_least_one_item_data = TableDataType.ITEM_DATA in (series_a_node_type, series_b_node_type)
    at_least_one_event_data = TableDataType.EVENT_DATA in (series_a_node_type, series_b_node_type)
    return at_least_one_event_data and at_least_one_item_data


def _get_event_data_id_of_item_series(item_series: SeriesT) -> PydanticObjectId:
    """
    Get the event data ID associated of the item series.

    Parameters
    ----------
    item_series: SeriesT
        item series

    Returns
    -------
    PydanticObjectId
        event data ID

    Raises
    ------
    ValueError
        raised if an event data ID cannot be found
    """
    for node in item_series.graph.iterate_nodes(
        target_node=item_series.node, node_type=NodeType.INPUT
    ):
        input_node = cast(InputNode, node)
        if input_node.parameters.type == TableDataType.ITEM_DATA:
            return input_node.parameters.event_data_id  # type: ignore
    raise ValueError("cannot find event data ID from series")


def _item_data_and_event_data_are_related(
    input_series_type: TableDataType,
    other_series_type: TableDataType,
    input_series: SeriesT,
    other_series: SeriesT,
) -> bool:
    """
    Checks if the item and event data are related.

    Parameters
    ----------
    input_series_type: TableDataType
        input series type
    other_series_type: TableDataType
        other series type
    input_series: SeriesT
        series
    other_series: SeriesT
        series

    Returns
    -------
    bool
        True, if item and event data are related, False otherwise
    """
    if not _is_one_item_and_one_event(input_series_type, other_series_type):
        return False
    item_data_series, event_data_series = _get_event_and_item_data_series(
        input_series_type, input_series, other_series
    )
    _, event_data_id = _series_data_type_and_tabular_id(event_data_series)
    event_data_id_of_item_series = _get_event_data_id_of_item_series(item_data_series)
    return event_data_id == event_data_id_of_item_series


def validate_feature_type(input_series: SeriesT, other_series: SeriesT) -> None:
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
    input_series_data_type, input_series_tabular_data_id = _series_data_type_and_tabular_id(
        input_series
    )
    other_series_data_type, other_series_tabular_data_id = _series_data_type_and_tabular_id(
        other_series
    )
    if (
        _both_are_lookup_features(input_series, other_series)
        or _is_from_same_data(
            input_series_data_type,
            other_series_data_type,
            input_series_tabular_data_id,
            other_series_tabular_data_id,
        )
        or _item_data_and_event_data_are_related(
            input_series_data_type, other_series_data_type, input_series, other_series
        )
    ):
        return
    raise ValueError("features are not of the right type")
