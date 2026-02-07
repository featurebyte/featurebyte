"""
Tests for ForecastPointSchema
"""

import pytest

from featurebyte.enum import DBVarType, TimeIntervalUnit
from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
from featurebyte.query_graph.model.timestamp_schema import TimeZoneColumn


def test_forecast_point_schema_with_global_timezone():
    """
    Test ForecastPointSchema with a global timezone string
    """
    schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        timezone="America/New_York",
    )
    assert schema.granularity == TimeIntervalUnit.DAY
    assert schema.dtype == DBVarType.DATE
    assert schema.timezone == "America/New_York"
    assert schema.has_timezone_column is False
    assert schema.timezone_column_name is None


def test_forecast_point_schema_with_timezone_column():
    """
    Test ForecastPointSchema with a timezone column reference
    """
    schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.DATE,
        is_utc_time=False,
        timezone=TimeZoneColumn(column_name="FORECAST_TIMEZONE", type="timezone"),
    )
    assert schema.has_timezone_column is True
    assert schema.timezone_column_name == "FORECAST_TIMEZONE"


def test_forecast_point_schema_week_granularity():
    """
    Test ForecastPointSchema with WEEK granularity (still uses DATE dtype)
    """
    schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.WEEK,
        dtype=DBVarType.DATE,
        timezone="UTC",
    )
    assert schema.granularity == TimeIntervalUnit.WEEK
    assert schema.dtype == DBVarType.DATE


def test_forecast_point_schema_hourly_with_timestamp():
    """
    Test ForecastPointSchema with HOUR granularity and TIMESTAMP dtype
    """
    schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.HOUR,
        dtype=DBVarType.TIMESTAMP,
        is_utc_time=True,
        timezone="Etc/UTC",
    )
    assert schema.granularity == TimeIntervalUnit.HOUR
    assert schema.dtype == DBVarType.TIMESTAMP
    assert schema.is_utc_time is True


def test_forecast_point_schema_varchar_requires_format_string():
    """
    Test that VARCHAR dtype requires format_string
    """
    with pytest.raises(ValueError) as exc:
        ForecastPointSchema(
            granularity=TimeIntervalUnit.DAY,
            dtype=DBVarType.VARCHAR,
            timezone="UTC",
        )
    assert "format_string is required when dtype is VARCHAR" in str(exc.value)


def test_forecast_point_schema_varchar_with_format_string():
    """
    Test ForecastPointSchema with VARCHAR dtype and format_string
    """
    schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        format_string="YYYY-MM-DD",
        is_utc_time=True,
        timezone="Etc/UTC",
    )
    assert schema.dtype == DBVarType.VARCHAR
    assert schema.format_string == "YYYY-MM-DD"


def test_forecast_point_schema_format_string_not_allowed_for_date():
    """
    Test that format_string is not allowed for DATE dtype
    """
    with pytest.raises(ValueError) as exc:
        ForecastPointSchema(
            granularity=TimeIntervalUnit.DAY,
            dtype=DBVarType.DATE,
            format_string="YYYY-MM-DD",
            timezone="UTC",
        )
    assert "format_string must not be provided for native date/timestamp types" in str(exc.value)


def test_forecast_point_schema_local_time_requires_timezone():
    """
    Test that local time (is_utc_time=False) requires timezone
    """
    with pytest.raises(ValueError) as exc:
        ForecastPointSchema(
            granularity=TimeIntervalUnit.DAY,
            dtype=DBVarType.DATE,
            is_utc_time=False,
        )
    assert "Timezone must be provided for local time forecast points" in str(exc.value)


def test_forecast_point_schema_invalid_dtype():
    """
    Test that INT dtype is not allowed for forecast point
    """
    with pytest.raises(ValueError) as exc:
        ForecastPointSchema(
            granularity=TimeIntervalUnit.DAY,
            dtype=DBVarType.INT,
            timezone="UTC",
        )
    assert "Invalid dtype INT for forecast point" in str(exc.value)


def test_forecast_point_schema_timestamp_tz():
    """
    Test ForecastPointSchema with TIMESTAMP_TZ dtype
    """
    schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.HOUR,
        dtype=DBVarType.TIMESTAMP_TZ,
        is_utc_time=True,
        timezone="Etc/UTC",
    )
    assert schema.dtype == DBVarType.TIMESTAMP_TZ
