"""
Helpers to derive partition filers from query graph
"""

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional

from bson import ObjectId
from dateutil.relativedelta import relativedelta
from sqlglot.expressions import Expression

from featurebyte.common.model_util import parse_duration_string
from featurebyte.enum import TimeIntervalUnit
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.window import CalendarWindow
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    AggregateAsAtParameters,
    BaseWindowAggregateParameters,
    LookupParameters,
    SnapshotsLookupParameters,
    TimeSeriesWindowAggregateParameters,
)
from featurebyte.query_graph.node.input import (
    InputNodeParameters,
    SCDTableInputNodeParameters,
    TimeSeriesTableInputNodeParameters,
)
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import PartitionColumnFilter, PartitionColumnFilters


@dataclass
class RelativeDeltaRange:
    """
    Represents a range defined by a relativedelta
    """

    value: relativedelta

    @classmethod
    def zero(cls) -> "RelativeDeltaRange":
        """
        Create a RelativeDeltaRange with zero value

        Returns
        -------
        RelativeDeltaRange
            A RelativeDeltaRange instance with zero relativedelta
        """
        return cls(value=relativedelta())


class InfiniteRange:
    """
    Represents an infinite range
    """


RangeType = RelativeDeltaRange | InfiniteRange


def merge_range(range1: RangeType, range2: RangeType) -> RangeType:
    """
    Merge two ranges, returning the larger one.

    Parameters
    ----------
    range1: RangeType
        The first range to compare.
    range2: RangeType
        The second range to compare.

    Returns
    -------
    RangeType
        The larger of the two ranges.
    """
    if isinstance(range1, InfiniteRange) or isinstance(range2, InfiniteRange):
        return InfiniteRange()
    reference_datetime = datetime(2025, 1, 1)
    if reference_datetime + range1.value > reference_datetime + range2.value:
        return range1
    return range2


@dataclass
class DataRequirements:
    """
    Data requirements relative to the minimum and maximum point in time for a source table.

    These ranges are derived from aggregate nodes and exclude the additional buffer that may be
    applied to final partition filters.

    before_min: RangeType
        The minimum range of data required before the minimum point in time.
    after_max: RangeType
        The maximum range of data required after the maximum point in time.
    """

    before_min: RangeType = field(default_factory=RelativeDeltaRange.zero)
    after_max: RangeType = field(default_factory=RelativeDeltaRange.zero)

    def merge(self, other: "DataRequirements") -> None:
        """
        Merge another DataRequirements into this one, updating the ranges to be the larger of the
        two.

        Parameters
        ----------
        other: DataRequirements
            The other DataRequirements to merge.
        """
        self.before_min = merge_range(self.before_min, other.before_min)
        self.after_max = merge_range(self.after_max, other.after_max)


def get_relativedelta_from_window_and_offset(
    window: CalendarWindow, offset: Optional[CalendarWindow]
) -> relativedelta:
    """
    Get relativedelta representing window of required data from feature derivation window and offset.

    Parameters
    ----------
    window: CalendarWindow
        The calendar window for feature derivation.
    offset: Optional[CalendarWindow]
        The calendar window for offset.

    Returns
    -------
    relativedelta
    """
    if window.is_fixed_size():
        seconds = window.to_seconds()
        if offset is not None:
            seconds += offset.to_seconds()
        delta = relativedelta(seconds=seconds)
    else:
        months = window.to_months()
        if offset is not None:
            months += offset.to_months()
        delta = relativedelta(months=months)
    return delta


def get_data_requirements_from_snapshots_parameters(
    snapshots_parameters: SnapshotsLookupParameters,
) -> DataRequirements:
    """
    Get data requirements from snapshots lookup parameters.

    Parameters
    ----------
    snapshots_parameters: SnapshotsLookupParameters
        The snapshots lookup parameters.

    Returns
    -------
    DataRequirements
    """
    feature_job_setting = snapshots_parameters.feature_job_setting
    if feature_job_setting is not None:
        blind_spot_window = feature_job_setting.get_blind_spot_calendar_window()
    else:
        blind_spot_window = None
    if blind_spot_window is None:
        blind_spot_window = CalendarWindow(
            unit=snapshots_parameters.time_interval.unit,
            size=0,
        )
    offset_size = snapshots_parameters.offset_size
    if offset_size is not None:
        offset_window = CalendarWindow(
            unit=snapshots_parameters.time_interval.unit,
            size=offset_size,
        )
    else:
        offset_window = None
    delta = get_relativedelta_from_window_and_offset(blind_spot_window, offset_window)
    return DataRequirements(before_min=RelativeDeltaRange(value=delta))


class DataRequirementsExtractor(ABC):
    """
    Extracts data requirements from query graph nodes.
    """

    def get_data_requirements(
        self, node: Node, input_node_parameters: InputNodeParameters
    ) -> Optional[DataRequirements]:
        """
        Get data requirements for a given node.

        Return None if the node does not impose any data requirements and will not influence the
        outcome of partition filtering.

        Parameters
        ----------
        node: Node
            The query graph node to extract data requirements from.
        input_node_parameters: InputNodeParameters
            Input node parameters for the primary input node.

        Returns
        -------
        Optional[DataRequirements]
            The data requirements for the node.
        """
        # Don't filter by partition for SCD tables
        if isinstance(input_node_parameters, SCDTableInputNodeParameters):
            return None

        return self.get_data_requirements_impl(node, input_node_parameters)

    @abstractmethod
    def get_data_requirements_impl(
        self, node: Node, input_node_parameters: InputNodeParameters
    ) -> Optional[DataRequirements]:
        """
        Implementation-specific data requirements extraction.

        Parameters
        ----------
        node: Node
            The query graph node to extract data requirements from.
        input_node_parameters: InputNodeParameters
            Input node parameters for the primary input node.

        Returns
        -------
        Optional[DataRequirements]
            The data requirements for the node.
        """


class WindowAggregateDataRequirementsExtractor(DataRequirementsExtractor):
    """
    Extracts data requirements from window aggregate nodes.
    """

    def get_data_requirements_impl(
        self, node: Node, input_node_parameters: InputNodeParameters
    ) -> Optional[DataRequirements]:
        parameters = node.parameters
        assert isinstance(parameters, BaseWindowAggregateParameters)
        _ = input_node_parameters
        out = DataRequirements()
        for window in parameters.windows:
            if window is None:
                return DataRequirements(before_min=InfiniteRange())
            duration_minutes = math.ceil(parse_duration_string(window) / 60)
            if parameters.offset is not None:
                duration_minutes += math.ceil(parse_duration_string(parameters.offset) / 60)
            window_requirements = DataRequirements(
                before_min=RelativeDeltaRange(value=relativedelta(minutes=duration_minutes)),
            )
            out.merge(window_requirements)
        return out


class TimeSeriesWindowAggregateDataRequirementsExtractor(DataRequirementsExtractor):
    """
    Extracts data requirements from time series window aggregate nodes.
    """

    def get_data_requirements_impl(
        self, node: Node, input_node_parameters: InputNodeParameters
    ) -> Optional[DataRequirements]:
        _ = input_node_parameters
        parameters = node.parameters
        assert isinstance(parameters, TimeSeriesWindowAggregateParameters)
        out = DataRequirements()
        for window in parameters.windows:
            delta = get_relativedelta_from_window_and_offset(window, parameters.offset)
            out.merge(DataRequirements(before_min=RelativeDeltaRange(value=delta)))
        return out


class LookupDataRequirementsExtractor(DataRequirementsExtractor):
    """
    Extracts data requirements from lookup nodes.
    """

    def get_data_requirements_impl(
        self, node: Node, input_node_parameters: InputNodeParameters
    ) -> Optional[DataRequirements]:
        _ = input_node_parameters
        parameters = node.parameters
        assert isinstance(parameters, LookupParameters)
        if parameters.snapshots_parameters is not None:
            # Lookup from snapshots table performs join using exact match, so we can discard data
            # prior to minimum point in time after adjusting for blind spot and offset
            return get_data_requirements_from_snapshots_parameters(parameters.snapshots_parameters)
        return None


class LookupTargetDataRequirementsExtractor(DataRequirementsExtractor):
    """
    Extracts data requirements from lookup target nodes.
    """

    def get_data_requirements_impl(
        self, node: Node, input_node_parameters: InputNodeParameters
    ) -> Optional[DataRequirements]:
        _ = input_node_parameters
        parameters = node.parameters
        assert isinstance(parameters, LookupParameters)
        if parameters.snapshots_parameters is not None:
            # No need to consider feature job setting's blind spot for lookup target, only the
            # offset. Extend data requirements to after maximum point in time by the offset size
            # since this is target derivation.
            offset_size = parameters.snapshots_parameters.offset_size
            if offset_size is not None:
                offset_window = CalendarWindow(
                    unit=parameters.snapshots_parameters.time_interval.unit,
                    size=offset_size,
                )
                delta = get_relativedelta_from_window_and_offset(offset_window, None)
                return DataRequirements(after_max=RelativeDeltaRange(value=delta))
            return DataRequirements()
        return None


class AggregateAsAtDataRequirementsExtractor(DataRequirementsExtractor):
    """
    Extracts data requirements from as-at aggregate nodes.
    """

    def get_data_requirements_impl(
        self, node: Node, input_node_parameters: InputNodeParameters
    ) -> Optional[DataRequirements]:
        _ = input_node_parameters
        parameters = node.parameters
        assert isinstance(parameters, AggregateAsAtParameters)
        if parameters.snapshots_parameters is not None:
            return get_data_requirements_from_snapshots_parameters(parameters.snapshots_parameters)
        return None


def get_data_requirements_extractor(node: Node) -> Optional[DataRequirementsExtractor]:
    """
    Get the appropriate DataRequirementsExtractor for a given node.

    Parameters
    ----------
    node: Node
        The query graph node to extract data requirements from.

    Returns
    -------
    Optional[DataRequirementsExtractor]
        The appropriate DataRequirementsExtractor for the node, or None if not applicable.
    """
    mapping = {
        NodeType.GROUPBY: WindowAggregateDataRequirementsExtractor,
        NodeType.TIME_SERIES_WINDOW_AGGREGATE: TimeSeriesWindowAggregateDataRequirementsExtractor,
        NodeType.LOOKUP: LookupDataRequirementsExtractor,
        NodeType.LOOKUP_TARGET: LookupTargetDataRequirementsExtractor,
        NodeType.AGGREGATE_AS_AT: AggregateAsAtDataRequirementsExtractor,
    }
    if node.type in mapping:
        return mapping[node.type]()  # type: ignore[abstract]
    return None


def get_default_partition_column_filter_buffer() -> TimeInterval:
    """
    Get the default buffer for partition filters when a conservative filtering is required

    Returns
    -------
    TimeInterval
    """
    return TimeInterval(unit=TimeIntervalUnit.DAY, value=7)


def convert_relativedelta_to_time_interval(delta: relativedelta) -> TimeInterval:
    """
    Convert a relativedelta to a TimeInterval.

    Parameters
    ----------
    delta: relativedelta
        The relativedelta to convert.

    Returns
    -------
    TimeInterval
        The corresponding TimeInterval.
    """
    # The relativedelta is either in months or minutes since we only produce these units in the
    # helper functions above. But relativedelta normalizes the values internally, so we need to
    # calculate the total months and minutes.
    total_months = delta.years * 12 + delta.months
    total_minutes = delta.days * 24 * 60 + delta.hours * 60 + delta.minutes
    if total_months:
        return TimeInterval(unit=TimeIntervalUnit.MONTH, value=total_months)
    return TimeInterval(unit=TimeIntervalUnit.MINUTE, value=total_minutes)


@dataclass
class InputNodeWindowInfo:
    """
    Information about the window for a specific input node.
    """

    table_id: ObjectId
    table_time_interval: Optional[TimeInterval]
    data_requirements: DataRequirements


def get_boundary_from_point_in_time(
    point_in_time_expr: Expression,
    range: RelativeDeltaRange,
    direction: Literal["backward", "forward"],
    adapter: BaseAdapter,
) -> Expression:
    """
    Get the boundary expression from a point in time expression and a relative delta range.

    Parameters
    ----------
    point_in_time_expr: Expression
        The point in time expression.
    range: RelativeDeltaRange
        The relative delta range.
    direction: Literal["backward", "forward"]
        The direction to apply the range. "backward" means subtracting the range from the
        point in time, while "forward" means adding the range to the point in time.
    adapter: BaseAdapter
        The SQL adapter to use for generating expressions.

    Returns
    -------
    Expression
    """
    if range == RelativeDeltaRange.zero():
        return point_in_time_expr
    time_interval = convert_relativedelta_to_time_interval(range.value)
    if direction == "forward":
        quantity = time_interval.value
    else:
        quantity = -1 * time_interval.value
    boundary = adapter.dateadd_time_interval(
        quantity_expr=make_literal_value(quantity),
        unit=time_interval.unit,
        timestamp_expr=point_in_time_expr,
    )
    return boundary


def get_partition_filters_from_graph(
    query_graph: QueryGraphModel,
    min_point_in_time: Expression,
    max_point_in_time: Expression,
    adapter: BaseAdapter,
) -> PartitionColumnFilters:
    """
    Get partition filters from the query graph.

    Parameters
    ----------
    query_graph: QueryGraphModel
        The query graph model
    min_point_in_time: Expression
        The minimum point in time to consider for partition filtering
    max_point_in_time: Expression
        The maximum point in time to consider for partition filtering
    adapter: BaseAdapter
        The SQL adapter to use for generating expressions

    Returns
    -------
    PartitionColumnFilters
        The partition column filters derived from the query graph.
    """
    input_node_infos = {}
    for node in query_graph.nodes:
        requirements_extractor = get_data_requirements_extractor(node)
        if not requirements_extractor:
            continue
        primary_input_nodes = QueryGraph.get_primary_input_nodes_from_graph_model(
            query_graph, node.name
        )
        for input_node in primary_input_nodes:
            current_requirements = requirements_extractor.get_data_requirements(
                node, input_node.parameters
            )
            if current_requirements is None:
                continue
            if input_node.name not in input_node_infos:
                parameters_dict = input_node.parameters.model_dump()
                table_id = ObjectId(parameters_dict.get("id"))
                assert table_id is not None
                if isinstance(input_node.parameters, TimeSeriesTableInputNodeParameters):
                    table_time_interval = input_node.parameters.time_interval
                else:
                    table_time_interval = None
                input_node_infos[input_node.name] = InputNodeWindowInfo(
                    table_id=table_id,
                    table_time_interval=table_time_interval,
                    data_requirements=current_requirements,
                )
            else:
                input_node_infos[input_node.name].data_requirements.merge(current_requirements)

    mapping = {}
    for input_node_info in input_node_infos.values():
        before_min = input_node_info.data_requirements.before_min
        if isinstance(before_min, RelativeDeltaRange):
            from_timestamp = get_boundary_from_point_in_time(
                point_in_time_expr=min_point_in_time,
                range=before_min,
                direction="backward",
                adapter=adapter,
            )
        else:
            continue
        after_max = input_node_info.data_requirements.after_max
        if isinstance(after_max, RelativeDeltaRange):
            to_timestamp = get_boundary_from_point_in_time(
                point_in_time_expr=max_point_in_time,
                range=after_max,
                direction="forward",
                adapter=adapter,
            )
        else:
            continue

        table_time_interval = input_node_info.table_time_interval
        if table_time_interval is not None and table_time_interval.unit == TimeIntervalUnit.MONTH:
            buffer = TimeInterval(unit=TimeIntervalUnit.MONTH, value=3)
        else:
            buffer = TimeInterval(unit=TimeIntervalUnit.MONTH, value=1)
        mapping[input_node_info.table_id] = PartitionColumnFilter(
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            buffer=buffer,
        )

    return PartitionColumnFilters(mapping=mapping)
