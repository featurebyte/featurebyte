"""
Target datetime accessor module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, TypeVar, Union

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.accessor.datetime import DatetimeAccessor

if TYPE_CHECKING:
    from featurebyte.api.target import Target
else:
    Target = TypeVar("Target")


class TargetDtAccessorMixin:
    """
    TargetDtAccessorMixin class
    """

    @property
    def dt(self: Target) -> DatetimeAccessor:  # type: ignore
        """
        dt accessor object

        Returns
        -------
        TargetDatetimeAccessor
        """
        return TargetDatetimeAccessor(self)


class TargetDatetimeAccessor(DatetimeAccessor):
    """
    DatetimeAccessor class used to manipulate datetime-like type Target object.

    This allows you to access the datetime-like properties of the Target values via the `.dt` attribute and the
    regular Target methods. The result will be a Target with the same index as the original Target.

    If the input target is a datetime-like type, the following properties are available:

    - year
    - quarter
    - month
    - week
    - day
    - day_of_week
    - hour
    - minute
    - second

    If the input target is a time delta type, the following properties are available:

    - day
    - hour
    - minute
    - second
    - millisecond
    - microsecond
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()

    @property
    def year(self) -> Target:
        """
        Returns the year component of each element.

        Returns
        -------
        Target
            Target containing the year component values

        Examples
        --------
        Compute the year component of a timestamp column:

        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_timestamp_year = target.dt.year
        """
        return super().year  # type: ignore[return-value]

    @property
    def quarter(self) -> Target:
        """
        Returns the quarter component of each element.

        Returns
        -------
        Target
            Target containing the quarter component values

        Examples
        --------
        Compute the quarter component of a timestamp column:

        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_timestamp_quarter = target.dt.quarter
        """
        return super().quarter  # type: ignore[return-value]

    @property
    def month(self) -> Target:
        """
        Returns the month component of each element.

        Returns
        -------
        Target
            Target containing the month component values

        Examples
        --------
        Compute the month component of a timestamp column:

        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_timestamp_month = target.dt.month
        """
        return super().month  # type: ignore[return-value]

    @property
    def week(self) -> Target:
        """
        Returns the week component of each element.

        Returns
        -------
        Target
            Target containing the week component values

        Examples
        --------
        Compute the week component of a timestamp column:

        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_timestamp_week = target.dt.week
        """
        return super().week  # type: ignore[return-value]

    @property
    def day(self) -> Target:
        """
        Returns the day component of each element.

        This is also available for Targets containing timedelta values, which is a result of taking
        the difference between two timestamp Targets.

        Returns
        -------
        Target
            Target containing the day of week component values

        Examples
        --------
        Compute the day component of a timestamp column:

        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_timestamp_day = target.dt.day
        """
        return super().day  # type: ignore[return-value]

    @property
    def day_of_week(self) -> Target:
        """
        Returns the day-of-week component of each element.

        The day of week is mapped to an integer value ranging from 0 (Monday) to 6 (Sunday).

        Returns
        -------
        Target
            Target containing the day of week component values

        Examples
        --------
        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_timestamp_day_of_week = target.dt.day_of_week
        """
        return super().day_of_week  # type: ignore[return-value]

    @property
    def hour(self) -> Target:
        """
        Returns the hour component of each element.

        This is also available for Targets containing timedelta values, which is a result of taking
        the difference between two timestamp Targets.

        Returns
        -------
        Target
            Target containing the hour component values

        Examples
        --------
        Compute the hour component of a timestamp column:

        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_timestamp_hour = target.dt.hour
        """
        return super().hour  # type: ignore[return-value]

    @property
    def minute(self) -> Target:
        """
        Returns the minute component of each element.

        This is also available for Targets containing timedelta values, which is a result of taking
        the difference between two timestamp Targets.

        Returns
        -------
        Target
            Target containing the minute component values

        Examples
        --------
        Compute the minute component of a timestamp column:

        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_timestamp_minute = target.dt.minute
        """
        return super().minute  # type: ignore[return-value]

    @property
    def second(self) -> Target:
        """
        Returns the second component of each element.

        This is also available for Targets containing timedelta values, which is a result of taking
        the difference between two timestamp Targets.

        Returns
        -------
        Target
            Target containing the second component values

        Examples
        --------

        Compute the second component of a timestamp column:

        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_timestamp_second = target.dt.second
        """
        return super().second  # type: ignore[return-value]

    @property
    def millisecond(self) -> Target:
        """
        Returns the millisecond component of each element.

        This is available only for Targets containing timedelta values, which is a result of taking
        the difference between two timestamp Targets.

        Returns
        -------
        Target
            Target containing the millisecond component values

        Examples
        --------

        Compute the millisecond component of a timestamp column:

        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_timestamp_millisecond = target.dt.millisecond  # doctest: +SKIP
        """
        return super().millisecond  # type: ignore[return-value]

    @property
    def microsecond(self) -> Target:
        """
        Returns the microsecond component of each element.

        This is available only for Targets containing timedelta values, which is a result of taking
        the difference between two timestamp Targets.

        Returns
        -------
        Target
            Target containing the microsecond component values

        Examples
        --------

        Compute the millisecond component of a timestamp column:

        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_timestamp_microsecond = target.dt.microsecond  # doctest: +SKIP
        """
        return super().microsecond  # type: ignore[return-value]

    def tz_offset(self, timezone_offset: Union[str, Target]) -> DatetimeAccessor:  # type: ignore[override]
        """
        Returns a DatetimeAccessor object with the specified timezone offset.

        The timezone offset will be applied to convert the underlying timestamp column to localized
        time before extracting datetime properties.

        Parameters
        ----------
        timezone_offset : str or Target
            The timezone offset to apply. If a string is provided, it must be a valid timezone
            offset in the format "(+|-)HH:mm". If the timezone offset can also be a column in the
            table, in which case a Target object should be provided.

        Returns
        -------
        DatetimeAccessor

        Examples
        --------
        >>> target = catalog.get_target("target_latest_invoice_timestamp")
        >>> target_tz_with_offset = target.dt.tz_offset("+08:00").hour
        """
        return super().tz_offset(timezone_offset)
