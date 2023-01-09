"""
Series validator module
"""
from __future__ import annotations

from typing import TYPE_CHECKING, List, Tuple, TypeVar

from featurebyte.api.entity import Entity
from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeType
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
    input_entity_parent_ids = [entity.data_id for entity in input_entity.parents]
    if other_entity_id in input_entity_parent_ids:
        return
    other_entity_parent_ids = [entity.data_id for entity in other_entity.parents]
    if input_entity_id in other_entity_parent_ids:
        return

    raise ValueError("entities are not the same type, and do not have a parent-child relationship")


def _series_data_type(input_series: SeriesT) -> TableDataType:
    """
    Get table data type for series

    Parameters
    ----------
    input_series: SeriesT
        input series

    Returns
    -------
    TableDataType
        table data type
    """
    operation_structure = input_series.graph.extract_operation_structure(input_series.node)
    # we only expect feature series to have a single column
    column_structure = operation_structure.columns[0]
    if isinstance(column_structure, DerivedDataColumn):
        return column_structure.columns[0].tabular_data_type
    # column_structure is a SourceDataColumn
    return column_structure.tabular_data_type


def _are_series_both_of_type(
    series_a: SeriesT, series_b: SeriesT, table_data_type: TableDataType
) -> bool:
    """
    Helper method to check if both series are of the same type

    Parameters
    ----------
    series_a: SeriesT
        series
    series_b: SeriesT
        series
    table_data_type: TableDataType
        data type that we want to check if both series are the type of

    Returns
    -------
    bool
        True, if both series are of the same specified data type, False otherwise.
    """
    return (
        _series_data_type(series_a) == table_data_type
        and _series_data_type(series_b) == table_data_type
    )


def _is_from_same_data(input_series: SeriesT, other_series: SeriesT) -> bool:
    """
    Checks if the two series are from the same item data or event data

    Parameters
    ----------
    input_series: SeriesT
        series
    other_series: SeriesT
        series

    Returns
    -------
    bool
        True, if both are from the same item data or event data, False otherwise
    """
    if not _are_series_both_of_type(
        input_series, other_series, TableDataType.ITEM_DATA
    ) and not _are_series_both_of_type(input_series, other_series, TableDataType.EVENT_DATA):
        return False
    input_node_parameters = input_series.node.parameters
    other_node_parameters = other_series.node.parameters
    input_id = input_node_parameters.id  # type: ignore
    other_id = other_node_parameters.id  # type: ignore
    return input_id == other_id


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


def _get_event_and_item_data(series_a: SeriesT, series_b: SeriesT) -> Tuple[SeriesT, SeriesT]:
    """
    Helper function to determine which series belongs to the item data, and which is the event data.

    This assumes that there is definitely one item data, and event data series. Use the other helper function
    _is_one_item_and_one_event beforehand.

    Parameters
    ----------
    series_a: SeriesT
        series
    series_b: SeriesT
        series

    Returns
    -------
    Tuple[SeriesT, SeriesT]
        (item data series, event data series)
    """
    if _series_data_type(series_a) == TableDataType.ITEM_DATA:
        return series_a, series_b
    return series_b, series_a


def _is_one_item_and_one_event(series_a: SeriesT, series_b: SeriesT) -> bool:
    """
    Helper function to determine if exactly one series is from an item data, and one is from an event data.

    Parameters
    ----------
    series_a: SeriesT
        series
    series_b: SeriesT
        series

    Returns
    -------
    bool
        True, if there's exactly one series that is from an item data, and one from an event data.
    """
    series_a_node_type = _series_data_type(series_a)
    series_b_node_type = _series_data_type(series_b)
    at_least_one_item_data = TableDataType.ITEM_DATA in (series_a_node_type, series_b_node_type)
    at_least_one_event_data = TableDataType.EVENT_DATA in (series_a_node_type, series_b_node_type)
    return at_least_one_event_data and at_least_one_item_data


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
    if not _is_one_item_and_one_event(input_series, other_series):
        return False
    item_data, event_data = _get_event_and_item_data(input_series, other_series)
    item_data_id = item_data.node.parameters.id  # type: ignore
    item_id_from_event_data = event_data.node.parameters.event_data_id
    return item_data_id == item_id_from_event_data


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
    """
    _validate_feature_type(input_series, other_series)
