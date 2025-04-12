"""
This module contains datetime accessor class
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Iterable, Optional, Union, cast

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.model_util import validate_timezone_offset_string
from featurebyte.core.util import series_binary_operation, series_unary_operation
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.input import (
    EventTableInputNodeParameters,
    InputNode,
    InputNodeParameters,
)
from featurebyte.query_graph.node.metadata.operation import (
    DerivedDataColumn,
    NodeOutputCategory,
    OperationStructure,
    SourceDataColumn,
)

if TYPE_CHECKING:
    from featurebyte.core.frame import Frame
    from featurebyte.core.series import FrozenSeries


class DtAccessorMixin:
    """
    DtAccessorMixin class
    """

    @property
    def dt(self: FrozenSeries) -> DatetimeAccessor:  # type: ignore
        """
        dt accessor object

        Returns
        -------
        DatetimeAccessor
        """
        return DatetimeAccessor(self)


class DatetimeAccessor:
    """
    DatetimeAccessor class used to manipulate datetime-like type FrozenSeries object.

    This allows you to access the datetime-like properties of the Series values via the `.dt` attribute and the
    regular Series methods. The result will be a Series with the same index as the original Series.

    If the input series is a datetime-like type, the following properties are available:

    - year
    - quarter
    - month
    - week
    - day
    - day_of_week
    - hour
    - minute
    - second

    If the input series is a time delta type, the following properties are available:

    - day
    - hour
    - minute
    - second
    - millisecond
    - microsecond

    Examples
    --------
    Getting the year from a time series

    >>> timeseries = series["timestamps"]  # doctest: +SKIP
    ... series["time_year"] = timeseries.dt.year
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Series")

    def __init__(
        self, obj: FrozenSeries, timezone_offset: Optional[Union[str, FrozenSeries]] = None
    ):
        if obj.is_datetime:
            self._node_type = NodeType.DT_EXTRACT
            self._property_node_params_map = {
                "year": "year",
                "quarter": "quarter",
                "month": "month",
                "week": "week",
                "day": "day",
                "day_of_week": "dayofweek",
                "hour": "hour",
                "minute": "minute",
                "second": "second",
            }
        elif obj.dtype == DBVarType.TIMEDELTA:
            self._node_type = NodeType.TIMEDELTA_EXTRACT
            self._property_node_params_map = {
                "day": "day",
                "hour": "hour",
                "minute": "minute",
                "second": "second",
                "millisecond": "millisecond",
                "microsecond": "microsecond",
            }
        else:
            raise AttributeError(
                f"Can only use .dt accessor with datetime or timedelta values; got {obj.dtype}"
            )
        self._obj = obj

        self._timezone_offset: Optional[Union[str, FrozenSeries]] = None

        # Optional timezone offset override
        if timezone_offset is not None:
            if not obj.is_datetime:
                raise ValueError("Cannot apply a timezone offset to a TIMEDELTA type column")
            if isinstance(timezone_offset, str):
                validate_timezone_offset_string(timezone_offset)
                self._timezone_offset = timezone_offset
            else:
                assert timezone_offset is not None
                if not timezone_offset.dtype == DBVarType.VARCHAR:
                    raise ValueError(
                        f"Only a string type column can be used as the timezone offset column; got {timezone_offset.dtype}"
                    )
                self._timezone_offset = timezone_offset

    def __dir__(self) -> Iterable[str]:
        # provide datetime extraction lookup and completion for __getattr__
        return self._property_node_params_map.keys()

    def tz_offset(self, timezone_offset: Union[str, FrozenSeries]) -> DatetimeAccessor:
        """
        Returns a DatetimeAccessor object with the specified timezone offset.

        The timezone offset will be applied to convert the underlying timestamp column to localized
        time before extracting datetime properties.

        Parameters
        ----------
        timezone_offset : str or FrozenSeries
            The timezone offset to apply. If a string is provided, it must be a valid timezone
            offset in the format "(+|-)HH:mm". If the timezone offset can also be a column in the
            table, in which case a Column object should be provided.

        Returns
        -------
        DatetimeAccessor

        Examples
        --------
        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["TimestampYearWithOffset"] = view["Timestamp"].dt.tz_offset("+08:00").year
        """
        return DatetimeAccessor(self._obj, timezone_offset)

    @property
    def year(self) -> FrozenSeries:
        """
        Returns the year component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the year component values

        Examples
        --------
        Compute the year component of a timestamp column:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["TimestampYear"] = view["Timestamp"].dt.year
        >>> view.preview(5).filter(regex="Timestamp")
                    Timestamp  TimestampYear
        0 2022-01-03 12:28:58           2022
        1 2022-01-03 16:32:15           2022
        2 2022-01-07 16:20:04           2022
        3 2022-01-10 16:18:32           2022
        4 2022-01-12 17:36:23           2022
        """
        return self._make_operation("year")

    @property
    def quarter(self) -> FrozenSeries:
        """
        Returns the quarter component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the quarter component values

        Examples
        --------
        Compute the quarter component of a timestamp column:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["TimestampQuarter"] = view["Timestamp"].dt.quarter
        >>> view.preview(5).filter(regex="Timestamp")
                    Timestamp  TimestampQuarter
        0 2022-01-03 12:28:58                 1
        1 2022-01-03 16:32:15                 1
        2 2022-01-07 16:20:04                 1
        3 2022-01-10 16:18:32                 1
        4 2022-01-12 17:36:23                 1
        """
        return self._make_operation("quarter")

    @property
    def month(self) -> FrozenSeries:
        """
        Returns the month component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the month component values

        Examples
        --------
        Compute the month component of a timestamp column:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["TimestampMonth"] = view["Timestamp"].dt.month
        >>> view.preview(5).filter(regex="Timestamp")
                    Timestamp  TimestampMonth
        0 2022-01-03 12:28:58               1
        1 2022-01-03 16:32:15               1
        2 2022-01-07 16:20:04               1
        3 2022-01-10 16:18:32               1
        4 2022-01-12 17:36:23               1
        """
        return self._make_operation("month")

    @property
    def week(self) -> FrozenSeries:
        """
        Returns the week component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the week component values

        Examples
        --------
        Compute the week component of a timestamp column:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["TimestampWeek"] = view["Timestamp"].dt.week
        >>> view.preview(5).filter(regex="Timestamp")
                    Timestamp  TimestampWeek
        0 2022-01-03 12:28:58              1
        1 2022-01-03 16:32:15              1
        2 2022-01-07 16:20:04              1
        3 2022-01-10 16:18:32              2
        4 2022-01-12 17:36:23              2
        """
        return self._make_operation("week")

    @property
    def day(self) -> FrozenSeries:
        """
        Returns the day component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the day of week component values

        Examples
        --------
        Compute the day component of a timestamp column:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["TimestampDay"] = view["Timestamp"].dt.day
        >>> view.preview(5).filter(regex="Timestamp")
                    Timestamp  TimestampDay
        0 2022-01-03 12:28:58             3
        1 2022-01-03 16:32:15             3
        2 2022-01-07 16:20:04             7
        3 2022-01-10 16:18:32            10
        4 2022-01-12 17:36:23            12


        Compute the interval since the previous event in terms of days:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["PreviousTimestamp"] = view["Timestamp"].lag("GroceryCustomerGuid")
        >>> view["DaysSincePreviousTimestamp"] = (
        ...     view["Timestamp"] - view["PreviousTimestamp"]
        ... ).dt.day
        >>> view.preview(5).filter(regex="Timestamp|Customer")
                            GroceryCustomerGuid           Timestamp   PreviousTimestamp  DaysSincePreviousTimestamp
        0  007a07da-1525-49be-94d1-fc7251f46a66 2022-01-07 12:02:17                 NaT                         NaN
        1  007a07da-1525-49be-94d1-fc7251f46a66 2022-01-11 19:46:41 2022-01-07 12:02:17                    4.322500
        2  007a07da-1525-49be-94d1-fc7251f46a66 2022-02-04 13:06:35 2022-01-11 19:46:41                   23.722153
        3  007a07da-1525-49be-94d1-fc7251f46a66 2022-03-09 18:51:16 2022-02-04 13:06:35                   33.239363
        4  007a07da-1525-49be-94d1-fc7251f46a66 2022-03-15 15:08:34 2022-03-09 18:51:16                    5.845347
        """
        return self._make_operation("day")

    @property
    def day_of_week(self) -> FrozenSeries:
        """
        Returns the day-of-week component of each element.

        The day of week is mapped to an integer value ranging from 0 (Monday) to 6 (Sunday).

        Returns
        -------
        FrozenSeries
            Column or Feature containing the day of week component values

        Examples
        --------
        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["TimestampDayOfWeek"] = view["Timestamp"].dt.day_of_week
        >>> view.preview(5).filter(regex="Timestamp")
                    Timestamp  TimestampDayOfWeek
        0 2022-01-03 12:28:58                   0
        1 2022-01-03 16:32:15                   0
        2 2022-01-07 16:20:04                   4
        3 2022-01-10 16:18:32                   0
        4 2022-01-12 17:36:23                   2
        """
        return self._make_operation("day_of_week")

    @property
    def hour(self) -> FrozenSeries:
        """
        Returns the hour component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the hour component values

        Examples
        --------
        Compute the hour component of a timestamp column:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["TimestampHour"] = view["Timestamp"].dt.hour
        >>> view.preview(5).filter(regex="Timestamp")
                    Timestamp  TimestampHour
        0 2022-01-03 12:28:58             12
        1 2022-01-03 16:32:15             16
        2 2022-01-07 16:20:04             16
        3 2022-01-10 16:18:32             16
        4 2022-01-12 17:36:23             17
        """
        return self._make_operation("hour")

    @property
    def minute(self) -> FrozenSeries:
        """
        Returns the minute component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the minute component values

        Examples
        --------
        Compute the minute component of a timestamp column:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["TimestampMinute"] = view["Timestamp"].dt.minute
        >>> view.preview(5).filter(regex="Timestamp")
                    Timestamp  TimestampMinute
        0 2022-01-03 12:28:58               28
        1 2022-01-03 16:32:15               32
        2 2022-01-07 16:20:04               20
        3 2022-01-10 16:18:32               18
        4 2022-01-12 17:36:23               36
        """
        return self._make_operation("minute")

    @property
    def second(self) -> FrozenSeries:
        """
        Returns the second component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the second component values

        Examples
        --------

        Compute the second component of a timestamp column:

        >>> view = catalog.get_view("GROCERYINVOICE")
        >>> view["TimestampSecond"] = view["Timestamp"].dt.second
        >>> view.preview(5).filter(regex="Timestamp")
                    Timestamp  TimestampSecond
        0 2022-01-03 12:28:58             58.0
        1 2022-01-03 16:32:15             15.0
        2 2022-01-07 16:20:04              4.0
        3 2022-01-10 16:18:32             32.0
        4 2022-01-12 17:36:23             23.0
        """
        return self._make_operation("second")

    @property
    def millisecond(self) -> FrozenSeries:
        """
        Returns the millisecond component of each element.

        This is available only for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the millisecond component values
        """
        return self._make_operation("millisecond")

    @property
    def microsecond(self) -> FrozenSeries:
        """
        Returns the microsecond component of each element.

        This is available only for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the microsecond component values
        """
        return self._make_operation("microsecond")

    def _make_operation(self, field_name: str) -> FrozenSeries:
        var_type = (
            DBVarType.FLOAT if self._node_type == NodeType.TIMEDELTA_EXTRACT else DBVarType.INT
        )
        if field_name not in self._property_node_params_map:
            raise ValueError(
                f"Datetime attribute {field_name} is not available for Series with"
                f" {self._obj.dtype} type. Available attributes:"
                f" {', '.join(self._property_node_params_map.keys())}."
            )
        node_params: dict[str, Any] = {"property": self._property_node_params_map[field_name]}
        timezone_offset = self._infer_timezone_offset(self._obj)

        if timezone_offset is not None and not isinstance(timezone_offset, str):
            return series_binary_operation(
                input_series=self._obj,
                other=timezone_offset,
                node_type=self._node_type,
                output_var_type=var_type,
                additional_node_params=node_params,
            )

        if isinstance(timezone_offset, str):
            node_params["timezone_offset"] = timezone_offset

        if self._node_type == NodeType.DT_EXTRACT:
            dtype_info = self._obj.dtype_info
            if dtype_info and dtype_info.metadata:
                node_params["timestamp_metadata"] = dtype_info.metadata

        return series_unary_operation(
            input_series=self._obj,
            node_type=self._node_type,
            output_var_type=var_type,
            node_params=node_params,
        )

    def _infer_timezone_offset(self, series: FrozenSeries) -> Optional[Union[FrozenSeries, str]]:
        """
        Infer the timezone offset for the given series.

        This will check whether the series is the event timestamp column of an EventTable where a
        timezone offset is specified. If this is the case, the timezone offset will be returned,
        either as a str (when the offset is a fixed value) or as a series (when the offset is a
        column in the EventTable). If no timezone offset can be inferred, None will be returned.

        Parameters
        ----------
        series: FrozenSeries
            The series to check for timezone offset

        Returns
        -------
        Optional[Union[FrozenSeries, str]]
        """

        # Only proceed if the series is a timestamp column and without TZ info
        if series.dtype != DBVarType.TIMESTAMP:
            return None

        if self._timezone_offset is not None:
            return self._timezone_offset

        operation_structure = series.graph.extract_operation_structure(
            series.node, keep_all_source_columns=True
        )
        if operation_structure.output_category != NodeOutputCategory.VIEW:
            return None

        # Check whether the series is a simple timestamp column (derived only from a timestamp
        # column from a source table)
        source_timestamp_column = self._get_source_timestamp_column(operation_structure)
        if source_timestamp_column is None or source_timestamp_column.table_id is None:
            return None

        # Check whether the series is the event timestamp column of an EventTable
        input_node_parameters = self._get_input_node_parameters_by_id(
            series.graph, source_timestamp_column.node_name, source_timestamp_column.table_id
        )
        if input_node_parameters.type == TableDataType.EVENT_TABLE:
            params = cast(EventTableInputNodeParameters, input_node_parameters)
            if params.timestamp_column == source_timestamp_column.name:
                # The series is an event timestamp column. Retrieve timezone offset if available.
                result: Optional[Union[FrozenSeries, str]] = None
                if params.event_timestamp_timezone_offset_column is not None:
                    result = self._get_offset_series_from_frame(
                        series.parent,
                        params.event_timestamp_timezone_offset_column,
                        params.id,
                    )
                elif params.event_timestamp_timezone_offset is not None:
                    result = params.event_timestamp_timezone_offset
                return result

        return None

    @classmethod
    def _get_source_timestamp_column(
        cls,
        operation_structure: OperationStructure,
    ) -> Optional[SourceDataColumn]:
        """
        Get a SourceDataColumn corresponding to timestamp column of interest

        Parameters
        ----------
        operation_structure: OperationStructure
            The operation structure to check for a timestamp column

        Returns
        -------
        Optional[SourceDataColumn]
        """
        source_columns_of_timestamp_type = []
        for item in operation_structure.iterate_source_columns_or_aggregations():
            if isinstance(item, SourceDataColumn):
                if item.dtype == DBVarType.TIMESTAMP:
                    source_columns_of_timestamp_type.append(item)

        # The timestamp offset will only be applied if the timestamp column is directly derived from
        # the event timestamp column. If there are more than one SourceDataColumn of timestamp type,
        # that is not the case, and this will return None to abort the timezone offset inference.
        if len(source_columns_of_timestamp_type) == 1:
            return source_columns_of_timestamp_type[0]

        return None

    @classmethod
    def _get_input_node_parameters_by_id(
        cls,
        graph: QueryGraph,
        target_node_name: str,
        table_id: PydanticObjectId,
    ) -> InputNodeParameters:
        """
        Get the parameters of an input node by its table_id

        Parameters
        ----------
        graph: QueryGraph
            QueryGraph to search for the input node
        target_node_name: str
            Target node to initiate the search
        table_id: PydanticObjectId
            Table identifier

        Returns
        -------
        InputNodeParameters
        """
        target_node = graph.get_node_by_name(target_node_name)
        for input_node in graph.iterate_nodes(target_node=target_node, node_type=NodeType.INPUT):
            input_node = cast(InputNode, input_node)
            if input_node.parameters.id == table_id:
                return input_node.parameters
        assert False, "Input node not found"

    @classmethod
    def _get_offset_series_from_frame(
        cls, frame: Frame, offset_column_name: str, table_id: Optional[PydanticObjectId]
    ) -> Optional[FrozenSeries]:
        """
        Get a series from a frame that corresponds to the timezone offset column

        Parameters
        ----------
        frame: Frame
            Frame to search for the offset column
        offset_column_name: str
            Name of the offset column as defined in the EventTable. The column might not necessarily
            have the same name in the frame (e.g. due to join suffix).
        table_id: Optional[PydanticObjectId]
            Table identifier

        Returns
        -------
        Optional[FrozenSeries]
        """

        operation_structure = frame.graph.extract_operation_structure(
            frame.node, keep_all_source_columns=True
        )
        offset_column_name_in_frame = None

        for opstruct_column in operation_structure.columns:
            if isinstance(opstruct_column, SourceDataColumn):
                if (
                    opstruct_column.table_id == table_id
                    and opstruct_column.name == offset_column_name
                ):
                    offset_column_name_in_frame = opstruct_column.name
                    break

            elif isinstance(opstruct_column, DerivedDataColumn):
                if cls._is_valid_timezone_offset_column_if_derived_column(
                    opstruct_column, table_id, offset_column_name
                ):
                    offset_column_name_in_frame = opstruct_column.name
                    break

        if offset_column_name_in_frame is not None:
            return frame[offset_column_name_in_frame]  # type: ignore

        return None

    @classmethod
    def _is_valid_timezone_offset_column_if_derived_column(
        cls,
        opstruct_column: DerivedDataColumn,
        table_id: Optional[PydanticObjectId],
        offset_column_name: str,
    ) -> bool:
        """
        Check whether a DerivedDataColumn is a valid timezone offset column

        Parameters
        ----------
        opstruct_column: DerivedDataColumn
            The DerivedDataColumn to check
        table_id: Optional[PydanticObjectId]
            Table identifier of the EventTable
        offset_column_name: str
            Name of the offset column as defined in the EventTable. The column might not necessarily
            have the same name in the frame (e.g. due to join suffix).

        Returns
        -------
        bool
        """
        is_derived_from_timezone_offset_column = False
        for column in opstruct_column.columns:
            if column.table_id == table_id and column.name == offset_column_name:
                is_derived_from_timezone_offset_column = True
                break

        if is_derived_from_timezone_offset_column:
            # These node types are allowed as they don't change the nature of the timezone offset
            # column. They arise from joins between tables (e.g. EventTable and ItemTable) as well
            # as cleaning operations.
            allowed_node_types = {NodeType.INPUT, NodeType.GRAPH, NodeType.JOIN}
            for node_name in opstruct_column.node_names:
                is_node_type_allowed = any(
                    node_name.startswith(allowed_node_type)
                    for allowed_node_type in allowed_node_types
                )
                if not is_node_type_allowed:
                    return False
            return True

        return False
