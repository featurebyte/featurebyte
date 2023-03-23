"""
This module contains datetime accessor class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType

if TYPE_CHECKING:
    from featurebyte.core.series import FrozenSeries


class DtAccessorMixin:
    """
    DtAccessorMixin class
    """

    @property
    def dt(self: FrozenSeries) -> DatetimeAccessor:  # type: ignore # pylint: disable=invalid-name
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

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["Series"],
        proxy_class="featurebyte.Series",
        accessor_name="dt",
    )

    def __init__(self, obj: FrozenSeries):
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

    def __dir__(self) -> Iterable[str]:
        # provide datetime extraction lookup and completion for __getattr__
        return self._property_node_params_map.keys()

    @property
    def year(self) -> FrozenSeries:
        """
        Return the year component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the year component values

        Examples
        --------
        Compute the year component of a timestamp column:

        >>> view = fb.Table.get("GROCERYINVOICE").get_view()
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
        Return the quarter component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the quarter component values

        Examples
        --------
        Compute the quarter component of a timestamp column:

        >>> view = fb.Table.get("GROCERYINVOICE").get_view()
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
        Return the day component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the day component values

        Examples
        --------
        Compute the month component of a timestamp column:

        >>> view = fb.Table.get("GROCERYINVOICE").get_view()
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
        Return the week component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the week component values

        Examples
        --------
        Compute the week component of a timestamp column:

        >>> view = fb.Table.get("GROCERYINVOICE").get_view()
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
        Return the day component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the day of week component values

        Examples
        --------
        Compute the day component of a timestamp column:

        >>> view = fb.Table.get("GROCERYINVOICE").get_view()
        >>> view["TimestampDay"] = view["Timestamp"].dt.day
        >>> view.preview(5).filter(regex="Timestamp")
                    Timestamp  TimestampDay
        0 2022-01-03 12:28:58             3
        1 2022-01-03 16:32:15             3
        2 2022-01-07 16:20:04             7
        3 2022-01-10 16:18:32            10
        4 2022-01-12 17:36:23            12
        """
        return self._make_operation("day")

    @property
    def day_of_week(self) -> FrozenSeries:
        """
        Return the day-of-week component of each element.

        The day of week is mapped to an integer value ranging from 0 (Monday) to 6 (Sunday).

        Returns
        -------
        FrozenSeries
            Column or Feature containing the day of week component values

        Examples
        --------
        >>> view = fb.Table.get("GROCERYINVOICE").get_view()
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
        Return the hour component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the hour component values

        Examples
        --------
        Compute the hour component of a timestamp column:

        >>> view = fb.Table.get("GROCERYINVOICE").get_view()
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
        Return the minute component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the minute component values

        Examples
        --------
        >>> view = fb.Table.get("GROCERYINVOICE").get_view()
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
        Return the second component of each element.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the second component values

        Examples
        --------

        Compute the second component of a timestamp column:

        >>> view = fb.Table.get("GROCERYINVOICE").get_view()
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
        Return the millisecond component of each element.

        This is available only for Series containing timedelta values, which is a result of taking
        the difference between two datetime Series.

        Returns
        -------
        FrozenSeries
            Column or Feature containing the millisecond component values
        """
        return self._make_operation("millisecond")

    @property
    def microsecond(self) -> FrozenSeries:
        """
        Return the microsecond component of each element.

        This is available only for Series containing timedelta values, which is a result of taking
        the difference between two datetime Series.

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
        return series_unary_operation(
            input_series=self._obj,
            node_type=self._node_type,
            output_var_type=var_type,
            node_params={"property": self._property_node_params_map[field_name]},
            **self._obj.unary_op_series_params(),
        )
