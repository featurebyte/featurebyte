"""
Tests for featurebyte/query_graph/model/timestamp_schema.py
"""

import pytest

from featurebyte.query_graph.model.timestamp_schema import TimestampSchema


def test_timezone_name__default():
    """
    Test default timezone name
    """
    timestamp_schema = TimestampSchema()
    assert timestamp_schema.timezone == "Etc/UTC"


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
