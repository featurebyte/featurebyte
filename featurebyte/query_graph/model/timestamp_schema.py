"""
Schema for timestamp columns
"""

from typing import Optional, Union

from pydantic_extra_types.timezone_name import TimeZoneName

from featurebyte.models.base import FeatureByteBaseModel


class TimeZoneOffsetColumn(FeatureByteBaseModel):
    """
    Represents a column that contains the timezone offset

    column_name: str
        Column name that contains the timezone offset
    """

    column_name: str


class TimestampSchema(FeatureByteBaseModel):
    """
    Schema for a timestamp column. To be embedded within a ColumnSpec

    format_string: Optional[str]
        Format string for the timestamp column represented as a string
    timezone: Optional[Union[TimezoneName, TimezoneOffsetColumn]]
        Timezone information for the timestamp column. If not provided, the timestamp should be
        interpreted as UTC
    """

    format_string: Optional[str] = None
    timezone: Optional[Union[TimeZoneName, TimeZoneOffsetColumn]] = None
