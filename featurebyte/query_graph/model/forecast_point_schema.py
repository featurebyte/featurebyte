"""
Schema for forecast point columns
"""

from typing import ClassVar, Optional

from pydantic import model_validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType, TimeIntervalUnit
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn, TimeZoneUnion


class ForecastPointSchema(FeatureByteBaseModel):
    """
    Schema for a forecast point column in observation tables.

    The forecast point represents the future date/time being predicted for,
    as opposed to POINT_IN_TIME which represents when the prediction is made.

    The FORECAST_POINT column is always string-based (DATE, TIMESTAMP, or VARCHAR),
    and the schema defines how to interpret the granularity and timezone.

    Parameters
    ----------
    granularity : TimeIntervalUnit
        The time granularity of the forecast point (DAY, WEEK, HOUR, etc.)
        For WEEK granularity, the column still contains date values (e.g., week start date)
    dtype : DBVarType
        The data type of the forecast point column (DATE, TIMESTAMP, TIMESTAMP_TZ, or VARCHAR)
    format_string : Optional[str]
        Database-specific format string if the column is stored as VARCHAR.
        Required for VARCHAR dtype, must not be provided for native date/timestamp types.
    is_utc_time : Optional[bool]
        Whether the forecast point values are in UTC (True) or local time (False)
    timezone : Optional[TimeZoneUnion]
        Global timezone setting, or reference to a FORECAST_TIMEZONE column.
        Required when is_utc_time is False.

    Examples
    --------
    **Example 1: Daily forecast with local timezone column**

    >>> forecast_schema = fb.ForecastPointSchema(
    ...     granularity=fb.TimeIntervalUnit.DAY,
    ...     dtype=fb.DBVarType.DATE,
    ...     is_utc_time=False,
    ...     timezone=fb.TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="timezone"),
    ... )

    **Example 2: Weekly forecast with global timezone (week start dates)**

    >>> forecast_schema = fb.ForecastPointSchema(
    ...     granularity=fb.TimeIntervalUnit.WEEK,
    ...     dtype=fb.DBVarType.DATE,
    ...     timezone="America/New_York",
    ... )

    **Example 3: Hourly forecast with VARCHAR format**

    >>> forecast_schema = fb.ForecastPointSchema(
    ...     granularity=fb.TimeIntervalUnit.HOUR,
    ...     dtype=fb.DBVarType.VARCHAR,
    ...     format_string="YYYY-MM-DD HH24:MI:SS",
    ...     is_utc_time=True,
    ...     timezone="Etc/UTC",
    ... )

    See Also
    --------
    - [TimestampSchema](/reference/featurebyte.query_graph.model.timestamp_schema.TimestampSchema/):
        Schema for a timestamp column that can include timezone information.
    - [Context](/reference/featurebyte.api.context.Context/):
        Context class that can include a forecast_point_schema.
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.ForecastPointSchema")

    granularity: TimeIntervalUnit
    dtype: DBVarType = DBVarType.DATE
    format_string: Optional[str] = None
    is_utc_time: Optional[bool] = None
    timezone: Optional[TimeZoneUnion] = None

    @model_validator(mode="after")
    def _validate_settings(self) -> "ForecastPointSchema":
        # Local time requires timezone
        if self.is_utc_time is False and self.timezone is None:
            raise ValueError("Timezone must be provided for local time forecast points")

        # Validate dtype
        if self.dtype not in DBVarType.supported_datetime_types():
            raise ValueError(
                f"Invalid dtype {self.dtype} for forecast point. "
                f"Must be one of: DATE, TIMESTAMP, TIMESTAMP_TZ, VARCHAR"
            )

        # format_string validation (similar to TimestampSchema)
        if self.dtype == DBVarType.VARCHAR and self.format_string is None:
            raise ValueError("format_string is required when dtype is VARCHAR")
        if (
            self.dtype in (DBVarType.DATE, DBVarType.TIMESTAMP, DBVarType.TIMESTAMP_TZ)
            and self.format_string is not None
        ):
            raise ValueError("format_string must not be provided for native date/timestamp types")

        return self

    @property
    def has_timezone_column(self) -> bool:
        """
        Whether the schema references a separate timezone column.

        Returns
        -------
        bool
        """
        return isinstance(self.timezone, TimeZoneColumn)

    @property
    def timezone_column_name(self) -> Optional[str]:
        """
        Name of the timezone column if it exists.

        Returns
        -------
        Optional[str]
            Name of the timezone column
        """
        if isinstance(self.timezone, TimeZoneColumn):
            return self.timezone.column_name
        return None
