"""
Tests for featurebyte/query_graph/model/timestamp_schema.py
"""

import pytest

from featurebyte import SourceType
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema


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
    assert "Invalid timezone name" in str(e)


@pytest.mark.parametrize(
    "source_type,format_string,expected_has_timezone",
    [
        (SourceType.SNOWFLAKE, "YYYY-MM-DD HH24:MI:SS.FF TZH", True),
        (SourceType.SNOWFLAKE, "YYYY-MM-DD HH24:MI:SS.FF TZH:TZM", True),
        (SourceType.SNOWFLAKE, "YYYY-MM-DD HH24:MI:SS.FF", False),
        (SourceType.SNOWFLAKE, None, False),
        (SourceType.BIGQUERY, "%Y-%m-%d %H:%M:%S %z", True),
        (SourceType.BIGQUERY, "%Y-%m-%d %H:%M:%S %Z", True),
        (SourceType.BIGQUERY, "%Y-%m-%d %H:%M:%S", False),
        (SourceType.BIGQUERY, None, False),
        (SourceType.SPARK, "yyyy-MM-dd HH:mm:ss VV", True),
        (SourceType.SPARK, "yyyy-MM-dd HH:mm:ss z", True),
        (SourceType.SPARK, "yyyy-MM-dd HH:mm:ss Z", True),
        (SourceType.SPARK, "yyyy-MM-dd HH:mm:ss", False),
        (SourceType.SPARK, None, False),
        (SourceType.DATABRICKS, "yyyy-MM-dd HH:mm:ss Z", True),
        (SourceType.DATABRICKS, "yyyy-MM-dd HH:mm:ss O", True),
        (SourceType.DATABRICKS, "yyyy-MM-dd HH:mm:ss X", True),
        (SourceType.DATABRICKS, "yyyy-MM-dd HH:mm:ss", False),
        (SourceType.DATABRICKS, None, False),
        (SourceType.DATABRICKS_UNITY, "yyyy-MM-dd HH:mm:ss X", True),
        (SourceType.DATABRICKS_UNITY, "yyyy-MM-dd HH:mm:ss x", True),
        (SourceType.DATABRICKS_UNITY, "yyyy-MM-dd HH:mm:ss", False),
        (SourceType.DATABRICKS_UNITY, None, False),
        (SourceType.SQLITE, "YYYY-MM-DD HH24:MI:SS.FF TZH", False),
        (SourceType.TEST, "%Y-%m-%d %H:%M:%S %Z", False),
    ],
)
def test_format_string_has_timezone(source_type, format_string, expected_has_timezone):
    """
    Test format string has timezone
    """
    timestamp_schema = TimestampSchema(format_string=format_string)
    assert timestamp_schema.format_string_has_timezone(source_type) == expected_has_timezone
