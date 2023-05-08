"""
Feature datetime accessor module.
"""
from typing import TYPE_CHECKING, TypeVar

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.accessor.datetime import DatetimeAccessor

if TYPE_CHECKING:
    from featurebyte.api.feature import Feature
else:
    Feature = TypeVar("Feature")


class FeatureDatetimeAccessor(DatetimeAccessor):
    """
    DatetimeAccessor class used to manipulate datetime-like type Feature object.

    This allows you to access the datetime-like properties of the Feature values via the `.dt` attribute and the
    regular Feature methods. The result will be a Feature with the same index as the original Feature.

    If the input feature is a datetime-like type, the following properties are available:

    - year
    - quarter
    - month
    - week
    - day
    - day_of_week
    - hour
    - minute
    - second

    If the input feature is a time delta type, the following properties are available:

    - day
    - hour
    - minute
    - second
    - millisecond
    - microsecond

    Examples
    --------
    Getting the year from a time series feature

    >>> timeseries = feature["timestamps"]  # doctest: +SKIP
    ... feature["time_year"] = timeseries.dt.year
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc()

    @property
    def year(self) -> Feature:
        """
        Returns the year component of each element.

        Returns
        -------
        Feature
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
        return super().year

    @property
    def quarter(self) -> Feature:
        """
        Returns the quarter component of each element.

        Returns
        -------
        Feature
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
        return super().quarter

    @property
    def month(self) -> Feature:
        """
        Returns the month component of each element.

        Returns
        -------
        Feature
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
        return super().month

    @property
    def week(self) -> Feature:
        """
        Returns the week component of each element.

        Returns
        -------
        Feature
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
        return super().week

    @property
    def day(self) -> Feature:
        """
        Returns the day component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
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
        >>> view["DaysSincePreviousTimestamp"] = (view["Timestamp"] - view["PreviousTimestamp"]).dt.day
        >>> view.preview(5).filter(regex="Timestamp|Customer")
                            GroceryCustomerGuid           Timestamp   PreviousTimestamp  DaysSincePreviousTimestamp
        0  007a07da-1525-49be-94d1-fc7251f46a66 2022-01-07 12:02:17                 NaT                         NaN
        1  007a07da-1525-49be-94d1-fc7251f46a66 2022-01-11 19:46:41 2022-01-07 12:02:17                    4.322500
        2  007a07da-1525-49be-94d1-fc7251f46a66 2022-02-04 13:06:35 2022-01-11 19:46:41                   23.722153
        3  007a07da-1525-49be-94d1-fc7251f46a66 2022-03-09 18:51:16 2022-02-04 13:06:35                   33.239363
        4  007a07da-1525-49be-94d1-fc7251f46a66 2022-03-15 15:08:34 2022-03-09 18:51:16                    5.845347
        """
        return super().day

    @property
    def day_of_week(self) -> Feature:
        """
        Returns the day-of-week component of each element.

        The day of week is mapped to an integer value ranging from 0 (Monday) to 6 (Sunday).

        Returns
        -------
        Feature
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
        return super().day_of_week

    @property
    def hour(self) -> Feature:
        """
        Returns the hour component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
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
        return super().hour

    @property
    def minute(self) -> Feature:
        """
        Returns the minute component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
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
        return super().minute

    @property
    def second(self) -> Feature:
        """
        Returns the second component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
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
        return super().second

    @property
    def millisecond(self) -> Feature:
        """
        Returns the millisecond component of each element.

        This is available only for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
            Column or Feature containing the millisecond component values
        """
        return super().millisecond

    @property
    def microsecond(self) -> Feature:
        """
        Returns the microsecond component of each element.

        This is available only for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
            Column or Feature containing the microsecond component values
        """
        return super().microsecond
