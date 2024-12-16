"""
Schema for timestamp columns
"""

from typing import ClassVar, Literal, Optional, Union

from pydantic_extra_types.timezone_name import TimeZoneName

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.base import FeatureByteBaseModel


class TimeZoneColumn(FeatureByteBaseModel):
    """
    Represents a column that contains the timezone information

    column_name: str
        Column name that contains the timezone offset
    """

    column_name: str
    type: Literal["offset", "timezone"]
    format_string: Optional[str] = None


class TimestampSchema(FeatureByteBaseModel):
    """
    Schema for a timestamp column. To be embedded within a ColumnSpec

    is_utc_time: bool
        Whether the timestamp values are in UTC (True) or local time (False)
    format_string: Optional[str]
        Format string for the timestamp column represented as a string
    timezone: Union[TimezoneName, TimezoneOffsetColumn]
        Timezone information for the timestamp column. The default value is "Etc/UTC"

    See Also
    --------
    - [create_time_series_table](/reference/featurebyte.api.source_table.SourceTable.create_time_series_table/): create time series table from source table
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TimestampSchema")

    format_string: Optional[str] = None
    is_utc_time: Optional[bool] = None
    timezone: Optional[Union[TimeZoneName, TimeZoneColumn]] = None
