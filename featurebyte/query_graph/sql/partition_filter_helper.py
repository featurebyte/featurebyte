"""
Helpers to derive partition filers from query graph
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from bson import ObjectId
from dateutil.relativedelta import relativedelta

from featurebyte.common.model_util import parse_duration_string
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.generic import (
    BaseWindowAggregateParameters,
    TimeSeriesWindowAggregateParameters,
)
from featurebyte.query_graph.node.input import SCDTableInputNodeParameters
from featurebyte.query_graph.sql.common import PartitionColumnFilter, PartitionColumnFilters


def get_relativedeltas_from_window_aggregate_params(
    parameters: BaseWindowAggregateParameters,
) -> Optional[list[relativedelta]]:
    """
    Get relativedeltas from window aggregate parameters.

    Returns
    -------
    Optional[list[relativedelta]]
        List of relativedelta objects if windows are valid, otherwise None.
    """
    out = []
    for window in parameters.windows:
        if window is None:
            return None
        duration_seconds = parse_duration_string(window)
        if parameters.offset is not None:
            duration_seconds += parse_duration_string(parameters.offset)
        out.append(relativedelta(seconds=duration_seconds))
    return out


def get_relativedeltas_from_time_series_params(
    parameters: TimeSeriesWindowAggregateParameters,
) -> Optional[list[relativedelta]]:
    """
    Get relativedeltas from time series window aggregate parameters.

    Returns
    -------
    Optional[list[relativedelta]]
        List of relativedelta objects if windows are valid, otherwise None.
    """
    out = []
    for window in parameters.windows:
        if window.is_fixed_size():
            seconds = window.to_seconds()
            if parameters.offset is not None:
                seconds += parameters.offset.to_seconds()
            delta = relativedelta(seconds=seconds)
        else:
            months = window.to_months()
            if parameters.offset is not None:
                months += parameters.offset.to_months()
            delta = relativedelta(months=months)
        out.append(delta)
    return out


def get_larger_window(current: Optional[relativedelta], new_value: relativedelta) -> relativedelta:
    """
    Get the larger of two relativedelta objects.

    Parameters
    ----------
    current: Optional[relativedelta]
        The current largest window, or None if no window has been set.
    new_value: relativedelta
        The new relativedelta value to compare.

    Returns
    -------
    relativedelta
        The larger of the two relativedelta objects.
    """
    reference_datetime = datetime(2025, 1, 1)
    if current is None:
        return new_value
    if reference_datetime + new_value > reference_datetime + current:
        return new_value
    return current


@dataclass
class InputNodeWindowInfo:
    """
    Information about the window for a specific input node.
    """

    table_id: ObjectId
    largest_window: Optional[relativedelta] = None
    has_unbounded_window: bool = False


def get_partition_filters_from_graph(
    query_graph: QueryGraphModel,
    min_point_in_time: datetime,
    max_point_in_time: datetime,
    buffer: Optional[relativedelta] = None,
) -> PartitionColumnFilters:
    """
    Get partition filters from the query graph.

    Parameters
    ----------
    query_graph: QueryGraphModel
        The query graph model
    min_point_in_time: datetime
        The minimum point in time to consider for partition filtering
    max_point_in_time: datetime
        The maximum point in time to consider for partition filtering
    buffer: Optional[relativedelta]
        A buffer to add to the partition filter range, defaults to 3 months if None
    """
    if buffer is None:
        buffer = relativedelta(months=3)

    input_node_infos = {}
    for node in query_graph.nodes:
        parameters = node.parameters
        primary_input_nodes = QueryGraph.get_primary_input_nodes_from_graph_model(
            query_graph, node.name
        )
        if isinstance(parameters, BaseWindowAggregateParameters):
            relativedeltas = get_relativedeltas_from_window_aggregate_params(parameters)
        elif isinstance(parameters, TimeSeriesWindowAggregateParameters):
            relativedeltas = get_relativedeltas_from_time_series_params(parameters)
        else:
            relativedeltas = []
        for input_node in primary_input_nodes:
            if isinstance(input_node.parameters, SCDTableInputNodeParameters):
                # Don't filter by partition for SCD tables
                continue
            if input_node.name not in input_node_infos:
                parameters_dict = input_node.parameters.model_dump()
                table_id = ObjectId(parameters_dict.get("id"))
                assert table_id is not None
                input_node_infos[input_node.name] = InputNodeWindowInfo(table_id=table_id)
            if relativedeltas is None:
                input_node_infos[input_node.name].has_unbounded_window = True
            else:
                for delta in relativedeltas:
                    input_node_infos[input_node.name].largest_window = get_larger_window(
                        input_node_infos[input_node.name].largest_window, delta
                    )

    mapping = {}
    for input_node_info in input_node_infos.values():
        if not input_node_info.has_unbounded_window and input_node_info.largest_window is not None:
            from_timestamp = (
                min_point_in_time + (-1 * input_node_info.largest_window) + (-1 * buffer)
            )
            to_timestamp = max_point_in_time + input_node_info.largest_window + buffer
            mapping[input_node_info.table_id] = PartitionColumnFilter(
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
            )

    return PartitionColumnFilters(mapping=mapping)
