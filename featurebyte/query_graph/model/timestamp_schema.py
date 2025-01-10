"""
Schema for timestamp columns
"""

from typing import ClassVar, Literal, Optional, Union

from pydantic import model_validator
from pydantic_extra_types.timezone_name import TimeZoneName

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.base import FeatureByteBaseModel


class TimeZoneColumn(FeatureByteBaseModel):
    """
    This class is used to specify how timezone data is stored within a dataset,
    either as a timezone offset or as a timezone name.

    Parameters
    ----------
    column_name : str
        The name of the column that contains the timezone information.
    type : Literal["offset", "timezone"]
        The type of timezone information stored in the column.
        - `"offset"`: Represents timezone as an offset from UTC (e.g., `+05:30`).
        - `"timezone"`: Represents timezone as a timezone name (e.g., `"America/New_York"`).

    See Also
    --------
    - [TimestampSchema](/reference/featurebyte.query_graph.model.timestamp_schema.TimestampSchema/):
        Schema for a timestamp column that can include timezone information.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TimeZoneColumn")

    # instance variables
    column_name: str
    type: Literal["offset", "timezone"]


TimeZoneUnion = Union[TimeZoneName, TimeZoneColumn]


class TimestampSchema(FeatureByteBaseModel):
    """
    Schema for a timestamp column. To be embedded within a ColumnSpec

    Parameters
    ----------
    is_utc_time: bool
        Whether the timestamp values are in UTC (True) or local time (False)
    format_string: Optional[str]
        Format string for the timestamp column represented as a string.
        Example formats:
        - `%Y-%m-%d %H:%M:%S`
        - `%d/%m/%Y %I:%M %p`
    timezone: Union[TimezoneName, TimezoneOffsetColumn]
        Timezone information for the timestamp column. The default value is `"Etc/UTC"`.
        You can specify timezones using:
        - Timezone names (e.g., `"America/New_York"`)
        - Timezone offset column (e.g., `TimeZoneColumn(column_name="timezone_offset", type="offset")`)

    See Also
    --------
    - [create_time_series_table](/reference/featurebyte.api.source_table.SourceTable.create_time_series_table/):
        Create time series table from source table
    - [TimeZoneColumn](/reference/featurebyte.query_graph.model.timestamp_schema.TimeZoneColumn/):
        Class for specifying how timezone data is stored within a dataset.

    Examples
    --------
    **Example 1: Basic UTC Timestamp**

    ```python
    utc_timestamp = fb.TimestampSchema(
        is_utc_time=True, format_string="%Y-%m-%dT%H:%M:%SZ", timezone="Etc/UTC"
    )
    ```

    **Example 2: Local Time with Specific Timezone**

    ```python
    local_timestamp = fb.TimestampSchema(
        is_utc_time=False, format_string="%d/%m/%Y %H:%M:%S", timezone="America/New_York"
    )
    ```

    **Example 3: Local Time with Timezone Offset Column**

    ```python
    offset_timestamp = fb.TimestampSchema(
        is_utc_time=False,
        format_string="%Y-%m-%d %H:%M:%S",
        timezone=fb.TimeZoneColumn(column_name="timezone_offset", type="offset"),
    )
    ```
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TimestampSchema")

    # instance variables
    format_string: Optional[str] = None
    is_utc_time: Optional[bool] = None
    timezone: Optional[TimeZoneUnion] = None

    @model_validator(mode="after")
    def _validate_settings(self) -> "TimestampSchema":
        if self.is_utc_time is False and self.timezone is None:
            raise ValueError("Timezone must be provided for local time")
        return self

    @property
    def has_timezone_offset_column(self) -> bool:
        """
        Whether the timestamp schema has a timezone offset column

        Returns
        -------
        bool
        """
        if self.timezone and isinstance(self.timezone, TimeZoneColumn):
            return True
        return False
