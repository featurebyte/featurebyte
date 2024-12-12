"""
Schema for timestamp columns
"""

from typing import ClassVar, Literal, Optional, Union

from pydantic_extra_types.timezone_name import TimeZoneName

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import SourceType
from featurebyte.models.base import FeatureByteBaseModel


class TimeZoneOffsetColumn(FeatureByteBaseModel):
    """
    Represents a column that contains the timezone offset

    column_name: str
        Column name that contains the timezone offset
    """

    column_name: str
    type: Literal["offset", "timezone"]
    format_string: str


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
    timezone: Optional[Union[TimeZoneName, TimeZoneOffsetColumn]] = None

    def format_string_has_timezone(self, source_type: SourceType) -> bool:
        """
        Whether the format string contains timezone information

        Parameters
        ----------
        source_type: SourceType
            Source type of the data source

        Returns
        -------
        bool
        """
        if self.format_string is None:
            return False

        if source_type == SourceType.SNOWFLAKE:
            # https://docs.snowflake.com/en/sql-reference/data-types-datetime
            offset_patterns = ["TZH", "TZM"]
        elif source_type in {SourceType.SPARK, SourceType.DATABRICKS, SourceType.DATABRICKS_UNITY}:
            # https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
            # https://docs.databricks.com/en/sql/language-manual/sql-ref-datetime-pattern.html
            offset_patterns = ["V", "z", "Z", "O", "X", "x"]
        elif source_type == SourceType.BIGQUERY:
            # https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time
            offset_patterns = ["%z", "%Z", "%Ez"]
        else:
            return False

        for pattern in offset_patterns:
            if pattern in self.format_string:
                return True
        return False
