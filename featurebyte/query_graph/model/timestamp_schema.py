"""
Schema for timestamp columns
"""

from typing import ClassVar, Literal, Optional, Union

from pydantic import model_validator
from pydantic_extra_types.timezone_name import TimeZoneName

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType
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
        - `"offset"`:
            Represents timezone as an offset from UTC (e.g., `+05:30`).
        - `"timezone"`:
            Represents timezone as a timezone name (e.g., `"America/New_York"`).
            The time zones are defined by the [International Time Zone Database](https://www.iana.org/time-zones)
            (commonly known as the IANA Time Zone Database or tz database).

    See Also
    --------
    - [TimestampSchema](/reference/featurebyte.query_graph.model.timestamp_schema.TimestampSchema/):
        Schema for a timestamp column that can include timezone information.
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TimeZoneColumn")

    column_name: str
    type: Literal["offset", "timezone"]


TimeZoneUnion = Union[TimeZoneName, TimeZoneColumn]


class TimestampSchema(FeatureByteBaseModel):
    """
    Schema for a timestamp column. To be embedded within a ColumnSpec

    Parameters
    ----------
    format_string: Optional[str]
        Format string for the timestamp column represented as a string. This format is specific to the underlying
        database and is used to parse the timestamp values.

        - **Databricks (Spark SQL):** (example: "yyyy-MM-dd HH:mm:ss")
            [Spark SQL Date and Timestamp Functions](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)

        - **Snowflake:** (example: "YYYY-MM-DD HH24:MI:SS")
            [Snowflake Date and Time Formats](https://docs.snowflake.com/en/sql-reference/functions-conversion.html#date-and-time-formats)

        - **BigQuery:** (example: "%Y-%m-%d %H:%M:%S")
            [BigQuery Date and Time Functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time)

        If the timestamp column is not of type string, `format_string` is not required and should not be provided.

    is_utc_time: Optional[bool]
        Whether the timestamp values are in UTC (True) or local time (False)

    timezone: Optional[Union[TimeZoneName, TimeZoneColumn]]
        Specifies the time zone information, which must conform to the
        [International Time Zone Database](https://www.iana.org/time-zones)
        (commonly known as the IANA Time Zone Database or tz database). The default value is None,
        which assumes "Etc/UTC". You can also provide a TimeZoneColumn to reference a dataset column
        containing time zone data.


    Examples
    --------
    **Example 1: Basic UTC Timestamp (BigQuery)**

    ```python
    utc_timestamp = fb.TimestampSchema(
        is_utc_time=True, format_string="%Y-%m-%dT%H:%M:%SZ", timezone="Etc/UTC"
    )
    ```
    **Example 2: Local Time with Specific Timezone (Databricks)**

    ```python
    local_timestamp = fb.TimestampSchema(
        is_utc_time=False, format_string="yyyy-MM-dd HH:mm:ss", timezone="America/New_York"
    )
    ```

    **Example 3: Local Time with Timezone Offset Column (Snowflake)**

    ```python
    offset_timestamp = fb.TimestampSchema(
        is_utc_time=False,
        format_string="YYYY-MM-DD HH24:MI:SS",
        timezone=fb.TimeZoneColumn(column_name="timezone_offset", type="offset"),
    )
    ```

    **Example 4: UTC Year-Month Timestamp (BigQuery)**

    ```python
    year_month_timestamp = fb.TimestampSchema(
        is_utc_time=True, format_string="%Y-%m", timezone="Etc/UTC"
    )
    ```

    See Also
    --------
    - [create_scd_table](/reference/featurebyte.api.source_table.SourceTable.create_scd_table/):
        Create slowly changing dimension table from source table
    - [create_time_series_table](/reference/featurebyte.api.source_table.SourceTable.create_time_series_table/):
        Create time series table from source table
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TimestampSchema")

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
        return self.timezone_offset_column_name is not None

    @property
    def timezone_offset_column_name(self) -> Optional[str]:
        """
        Name of the timezone offset column if it exists

        Returns
        -------
        Optional[str]
            Name of the timezone offset column
        """
        if self.timezone and isinstance(self.timezone, TimeZoneColumn):
            return self.timezone.column_name
        return None


class ExtendedTimestampSchema(TimestampSchema):
    """Extended timestamp schema with additional properties"""

    dtype: DBVarType


class TimezoneOffsetSchema(FeatureByteBaseModel):
    """Timezone offset column schema"""

    dtype: DBVarType


class TimestampTupleSchema(FeatureByteBaseModel):
    """Schema for a tuple of timestamp columns. To be embedded within a ColumnSpec"""

    timestamp_schema: ExtendedTimestampSchema
    timezone_offset_schema: TimezoneOffsetSchema
