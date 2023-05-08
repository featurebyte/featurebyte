"""
Feature datetime accessor module.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.accessor.datetime import DatetimeAccessor

if TYPE_CHECKING:
    from featurebyte.api.feature import Feature
else:
    Feature = TypeVar("Feature")


class FeatureDtAccessorMixin:
    """
    FeatureDtAccessorMixin class
    """

    @property
    def dt(self: Feature) -> FeatureDatetimeAccessor:  # type: ignore # pylint: disable=invalid-name
        """
        dt accessor object

        Returns
        -------
        FeatureDatetimeAccessor
        """
        return FeatureDatetimeAccessor(self)


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
            Feature containing the year component values

        Examples
        --------
        Compute the year component of a timestamp column:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["TimestampYear"] = feature["Timestamp"].dt.year  # doctest: +SKIP
        >>> feature.preview(5).filter(regex="Timestamp")  # doctest: +SKIP
                    Timestamp  TimestampYear
        0 2022-01-03 12:28:58           2022
        1 2022-01-03 16:32:15           2022
        2 2022-01-07 16:20:04           2022
        3 2022-01-10 16:18:32           2022
        4 2022-01-12 17:36:23           2022
        """
        return super().year  # type: ignore[return-value]

    @property
    def quarter(self) -> Feature:
        """
        Returns the quarter component of each element.

        Returns
        -------
        Feature
            Feature containing the quarter component values

        Examples
        --------
        Compute the quarter component of a timestamp column:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["TimestampQuarter"] = feature["Timestamp"].dt.quarter  # doctest: +SKIP
        >>> feature.preview(5).filter(regex="Timestamp")  # doctest: +SKIP
                    Timestamp  TimestampQuarter
        0 2022-01-03 12:28:58                 1
        1 2022-01-03 16:32:15                 1
        2 2022-01-07 16:20:04                 1
        3 2022-01-10 16:18:32                 1
        4 2022-01-12 17:36:23                 1
        """
        return super().quarter  # type: ignore[return-value]

    @property
    def month(self) -> Feature:
        """
        Returns the month component of each element.

        Returns
        -------
        Feature
            Feature containing the month component values

        Examples
        --------
        Compute the month component of a timestamp column:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["TimestampMonth"] = feature["Timestamp"].dt.month  # doctest: +SKIP
        >>> feature.preview(5).filter(regex="Timestamp")  # doctest: +SKIP
                    Timestamp  TimestampMonth
        0 2022-01-03 12:28:58               1
        1 2022-01-03 16:32:15               1
        2 2022-01-07 16:20:04               1
        3 2022-01-10 16:18:32               1
        4 2022-01-12 17:36:23               1
        """
        return super().month  # type: ignore[return-value]

    @property
    def week(self) -> Feature:
        """
        Returns the week component of each element.

        Returns
        -------
        Feature
            Feature containing the week component values

        Examples
        --------
        Compute the week component of a timestamp column:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["TimestampWeek"] = feature["Timestamp"].dt.week  # doctest: +SKIP
        >>> feature.preview(5).filter(regex="Timestamp")  # doctest: +SKIP
                    Timestamp  TimestampWeek
        0 2022-01-03 12:28:58              1
        1 2022-01-03 16:32:15              1
        2 2022-01-07 16:20:04              1
        3 2022-01-10 16:18:32              2
        4 2022-01-12 17:36:23              2
        """
        return super().week  # type: ignore[return-value]

    @property
    def day(self) -> Feature:
        """
        Returns the day component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
            Feature containing the day of week component values

        Examples
        --------
        Compute the day component of a timestamp column:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["TimestampDay"] = feature["Timestamp"].dt.day  # doctest: +SKIP
        >>> feature.preview(5).filter(regex="Timestamp")  # doctest: +SKIP
                    Timestamp  TimestampDay
        0 2022-01-03 12:28:58             3
        1 2022-01-03 16:32:15             3
        2 2022-01-07 16:20:04             7
        3 2022-01-10 16:18:32            10
        4 2022-01-12 17:36:23            12


        Compute the interval since the previous event in terms of days:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["PreviousTimestamp"] = feature["Timestamp"].lag("GroceryCustomerGuid")  # doctest: +SKIP
        >>> feature["DaysSincePreviousTimestamp"] = (feature["Timestamp"] - feature["PreviousTimestamp"]).dt.day  # doctest: +SKIP
        >>> feature.preview(5).filter(regex="Timestamp|Customer")  # doctest: +SKIP
                            GroceryCustomerGuid           Timestamp   PreviousTimestamp  DaysSincePreviousTimestamp
        0  007a07da-1525-49be-94d1-fc7251f46a66 2022-01-07 12:02:17                 NaT                         NaN
        1  007a07da-1525-49be-94d1-fc7251f46a66 2022-01-11 19:46:41 2022-01-07 12:02:17                    4.322500
        2  007a07da-1525-49be-94d1-fc7251f46a66 2022-02-04 13:06:35 2022-01-11 19:46:41                   23.722153
        3  007a07da-1525-49be-94d1-fc7251f46a66 2022-03-09 18:51:16 2022-02-04 13:06:35                   33.239363
        4  007a07da-1525-49be-94d1-fc7251f46a66 2022-03-15 15:08:34 2022-03-09 18:51:16                    5.845347
        """
        return super().day  # type: ignore[return-value]

    @property
    def day_of_week(self) -> Feature:
        """
        Returns the day-of-week component of each element.

        The day of week is mapped to an integer value ranging from 0 (Monday) to 6 (Sunday).

        Returns
        -------
        Feature
            Feature containing the day of week component values

        Examples
        --------
        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["TimestampDayOfWeek"] = feature["Timestamp"].dt.day_of_week  # doctest: +SKIP
        >>> feature.preview(5).filter(regex="Timestamp")  # doctest: +SKIP
                    Timestamp  TimestampDayOfWeek
        0 2022-01-03 12:28:58                   0
        1 2022-01-03 16:32:15                   0
        2 2022-01-07 16:20:04                   4
        3 2022-01-10 16:18:32                   0
        4 2022-01-12 17:36:23                   2
        """
        return super().day_of_week  # type: ignore[return-value]

    @property
    def hour(self) -> Feature:
        """
        Returns the hour component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
            Feature containing the hour component values

        Examples
        --------
        Compute the hour component of a timestamp column:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["TimestampHour"] = feature["Timestamp"].dt.hour  # doctest: +SKIP
        >>> feature.preview(5).filter(regex="Timestamp")  # doctest: +SKIP
                    Timestamp  TimestampHour
        0 2022-01-03 12:28:58             12
        1 2022-01-03 16:32:15             16
        2 2022-01-07 16:20:04             16
        3 2022-01-10 16:18:32             16
        4 2022-01-12 17:36:23             17
        """
        return super().hour  # type: ignore[return-value]

    @property
    def minute(self) -> Feature:
        """
        Returns the minute component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
            Feature containing the minute component values

        Examples
        --------
        Compute the minute component of a timestamp column:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["TimestampMinute"] = feature["Timestamp"].dt.minute  # doctest: +SKIP
        >>> feature.preview(5).filter(regex="Timestamp")  # doctest: +SKIP
                    Timestamp  TimestampMinute
        0 2022-01-03 12:28:58               28
        1 2022-01-03 16:32:15               32
        2 2022-01-07 16:20:04               20
        3 2022-01-10 16:18:32               18
        4 2022-01-12 17:36:23               36
        """
        return super().minute  # type: ignore[return-value]

    @property
    def second(self) -> Feature:
        """
        Returns the second component of each element.

        This is also available for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
            Feature containing the second component values

        Examples
        --------

        Compute the second component of a timestamp column:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["TimestampSecond"] = feature["Timestamp"].dt.second  # doctest: +SKIP
        >>> feature.preview(5).filter(regex="Timestamp")  # doctest: +SKIP
                    Timestamp  TimestampSecond
        0 2022-01-03 12:28:58             58.0
        1 2022-01-03 16:32:15             15.0
        2 2022-01-07 16:20:04              4.0
        3 2022-01-10 16:18:32             32.0
        4 2022-01-12 17:36:23             23.0
        """
        return super().second  # type: ignore[return-value]

    @property
    def millisecond(self) -> Feature:
        """
        Returns the millisecond component of each element.

        This is available only for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
            Feature containing the millisecond component values

        Examples
        --------

        Compute the millisecond component of a timestamp column:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["TimestampMillisecond"] = feature["Timestamp"].dt.millisecond  # doctest: +SKIP
        """
        return super().millisecond  # type: ignore[return-value]

    @property
    def microsecond(self) -> Feature:
        """
        Returns the microsecond component of each element.

        This is available only for Series containing timedelta values, which is a result of taking
        the difference between two timestamp Series.

        Returns
        -------
        Feature
            Feature containing the microsecond component values

        Examples
        --------

        Compute the millisecond component of a timestamp column:

        >>> feature = catalog.get_feature("InvoiceCount_60days")
        >>> feature["TimestampMicrosecond"] = feature["Timestamp"].dt.microsecond  # doctest: +SKIP
        """
        return super().microsecond  # type: ignore[return-value]
