"""
Feature datetime accessor module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, TypeVar, Union

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
    def dt(self: Feature) -> DatetimeAccessor:  # type: ignore
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
    """

    # documentation metadata
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()

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

        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampYear"] = feature.dt.year
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

        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampQuarter"] = feature.dt.quarter
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

        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampMonth"] = feature.dt.month
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

        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampWeek"] = feature.dt.week
        """
        return super().week  # type: ignore[return-value]

    @property
    def day(self) -> Feature:
        """
        Returns the day component of each element.

        This is also available for Features containing timedelta values, which is a result of taking
        the difference between two timestamp Features.

        Returns
        -------
        Feature
            Feature containing the day of week component values

        Examples
        --------
        Compute the day component of a timestamp column:

        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampDay"] = feature.dt.day
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
        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampDayOfWeek"] = feature.dt.day_of_week
        """
        return super().day_of_week  # type: ignore[return-value]

    @property
    def hour(self) -> Feature:
        """
        Returns the hour component of each element.

        This is also available for Features containing timedelta values, which is a result of taking
        the difference between two timestamp Features.

        Returns
        -------
        Feature
            Feature containing the hour component values

        Examples
        --------
        Compute the hour component of a timestamp column:

        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampHour"] = feature.dt.hour
        """
        return super().hour  # type: ignore[return-value]

    @property
    def minute(self) -> Feature:
        """
        Returns the minute component of each element.

        This is also available for Features containing timedelta values, which is a result of taking
        the difference between two timestamp Features.

        Returns
        -------
        Feature
            Feature containing the minute component values

        Examples
        --------
        Compute the minute component of a timestamp column:

        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampMinute"] = feature.dt.minute
        """
        return super().minute  # type: ignore[return-value]

    @property
    def second(self) -> Feature:
        """
        Returns the second component of each element.

        This is also available for Features containing timedelta values, which is a result of taking
        the difference between two timestamp Features.

        Returns
        -------
        Feature
            Feature containing the second component values

        Examples
        --------

        Compute the second component of a timestamp column:

        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampSecond"] = feature.dt.second
        """
        return super().second  # type: ignore[return-value]

    @property
    def millisecond(self) -> Feature:
        """
        Returns the millisecond component of each element.

        This is available only for Features containing timedelta values, which is a result of taking
        the difference between two timestamp Features.

        Returns
        -------
        Feature
            Feature containing the millisecond component values

        Examples
        --------

        Compute the millisecond component of a timestamp column:

        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampMillisecond"] = feature.dt.millisecond  # doctest: +SKIP
        """
        return super().millisecond  # type: ignore[return-value]

    @property
    def microsecond(self) -> Feature:
        """
        Returns the microsecond component of each element.

        This is available only for Features containing timedelta values, which is a result of taking
        the difference between two timestamp Features.

        Returns
        -------
        Feature
            Feature containing the microsecond component values

        Examples
        --------

        Compute the millisecond component of a timestamp column:

        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampMicrosecond"] = feature.dt.microsecond  # doctest: +SKIP
        """
        return super().microsecond  # type: ignore[return-value]

    def tz_offset(self, timezone_offset: Union[str, Feature]) -> DatetimeAccessor:  # type: ignore[override]
        """
        Returns a DatetimeAccessor object with the specified timezone offset.

        The timezone offset will be applied to convert the underlying timestamp column to localized
        time before extracting datetime properties.

        Parameters
        ----------
        timezone_offset : str or Feature
            The timezone offset to apply. If a string is provided, it must be a valid timezone
            offset in the format "(+|-)HH:mm". If the timezone offset can also be a column in the
            table, in which case a Feature object should be provided.

        Returns
        -------
        DatetimeAccessor

        Examples
        --------
        >>> feature = catalog.get_feature("CustomerLatestInvoiceTimestamp")
        >>> feature_group = fb.FeatureGroup([feature])
        >>> feature_group["TimestampWithOffset"] = feature.dt.tz_offset("+08:00").hour
        """
        return super().tz_offset(timezone_offset)
