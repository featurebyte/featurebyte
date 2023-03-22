"""
This module contains datetime accessor class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

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
        >>> view["TimestampDayOfWeek"] = view["Timestamp"].dt.hour
        >>> view.preview(5).filter(regex="Timestamp")
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
        >>> view = fb.Table.get("GROCERYINVOICE").get_view()
        >>> view["TimestampHour"] = view["Timestamp"].dt.hour
        >>> view.preview(5).filter(regex="Timestamp")
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
        >>> view = fb.Table.get("GROCERYINVOICE").get_view()
        >>> view["TimestampSecond"] = view["Timestamp"].dt.second
        >>> view.preview(5).filter(regex="Timestamp")
        """
        return self._make_operation("second")

    def _make_operation(self, field_name: str) -> FrozenSeries:
        var_type = (
            DBVarType.FLOAT if self._node_type == NodeType.TIMEDELTA_EXTRACT else DBVarType.INT
        )
        if field_name not in self._property_node_params_map:
            raise ValueError(
                f"Datetime attribute {field_name} is not available for {self.obj}. Available"
                f" attributes are: {', '.join(self._property_node_params_map.keys())}"
            )
        return series_unary_operation(
            input_series=self._obj,
            node_type=self._node_type,
            output_var_type=var_type,
            node_params={"property": self._property_node_params_map[field_name]},
            **self._obj.unary_op_series_params(),
        )

    def __getattr__(self, item: str) -> Any:
        try:
            return object.__getattribute__(self, item)
        except AttributeError as exc:
            var_type = (
                DBVarType.FLOAT if self._node_type == NodeType.TIMEDELTA_EXTRACT else DBVarType.INT
            )
            if item in self._property_node_params_map:
                return series_unary_operation(
                    input_series=self._obj,
                    node_type=self._node_type,
                    output_var_type=var_type,
                    node_params={"property": self._property_node_params_map[item]},
                    **self._obj.unary_op_series_params(),
                )
            raise exc
