"""
Tests for featurebyte/query_graph/model/timestamp_schema.py
"""

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.schema import ColumnSpec


@pytest.fixture(name="ts_schema_utc_without_timezone")
def ts_schema_utc_without_timezone_fixture():
    """Timestamp schema with UTC time and no timezone"""
    return TimestampSchema(format_string=None, is_utc_time=True, timezone=None)


@pytest.fixture(name="ts_schema_utc_with_timezone")
def ts_schema_utc_with_timezone_fixture():
    """Timestamp schema with UTC time and timezone"""
    return TimestampSchema(format_string=None, is_utc_time=True, timezone="EST")


@pytest.fixture(name="ts_schema_local_with_timezone")
def ts_schema_local_with_timezone_fixture():
    """Timestamp schema with UTC time and timezone"""
    return TimestampSchema(format_string=None, is_utc_time=True, timezone="EST")


def test_timezone_name__default():
    """
    Test default timezone name
    """
    timestamp_schema = TimestampSchema()
    assert timestamp_schema.timezone is None


def test_timezone_name__valid():
    """
    Test valid timezone name
    """
    timestamp_schema = TimestampSchema(timezone="America/New_York")
    assert timestamp_schema.timezone == "America/New_York"


def test_timezone_name__invalid():
    """
    Test invalid timezone name
    """
    with pytest.raises(ValueError) as e:
        TimestampSchema(timezone="my_timezone")
    assert "Invalid timezone name" in str(e.value)


def test_local_time_without_timezone():
    """
    Test setting local time without timezone
    """
    with pytest.raises(ValueError) as e:
        TimestampSchema(is_utc_time=False, timezone=None)
    assert "Timezone must be provided for local time" in str(e.value)


def test_invalid_ts_datetime_types(ts_schema_utc_without_timezone):
    """
    Test invalid timestamp schema datetime types
    """
    with pytest.raises(ValueError) as e:
        ColumnSpec(
            name="ts",
            dtype=DBVarType.INT,
            dtype_metadata=DBVarTypeMetadata(timestamp_schema=ts_schema_utc_without_timezone),
        )
    assert "Column ts is of type INT (expected: [DATE, TIMESTAMP, VARCHAR])" in str(e.value)


def test_string_datetime_missing_format_string(ts_schema_utc_without_timezone):
    """
    Test string timestamp type missing format_string
    """
    with pytest.raises(ValueError) as e:
        ColumnSpec(
            name="ts",
            dtype=DBVarType.VARCHAR,
            dtype_metadata=DBVarTypeMetadata(timestamp_schema=TimestampSchema()),
        )
    assert "format_string is required in the timestamp_schema for column ts" in str(e.value)


def test_non_string_datetime_with_format_string(ts_schema_utc_without_timezone):
    """
    Test non-string timestamp type with unnecessary format_string
    """
    with pytest.raises(ValueError) as e:
        ColumnSpec(
            name="ts",
            dtype=DBVarType.TIMESTAMP,
            dtype_metadata=DBVarTypeMetadata(
                timestamp_schema=TimestampSchema(format_string="YYYY-MM-DD")
            ),
        )
    assert "format_string is not supported in the timestamp_schema for non-string column ts" in str(
        e.value
    )
