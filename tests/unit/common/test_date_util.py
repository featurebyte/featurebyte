"""
Tests for functions in date_util.py module
"""
from datetime import datetime, timedelta, timezone

import pytest

from featurebyte.common import date_util


def test_timestamp_to_tile_index(mock_snowflake_tile):
    """Test convert timestamp to tile_index"""
    dt = datetime(2022, 1, 1, tzinfo=timezone.utc)
    tile_ind = date_util.timestamp_to_tile_index(dt, mock_snowflake_tile)
    assert tile_ind == 5469983


def test_timestamp_to_tile_index_no_timezone(mock_snowflake_tile):
    """Test convert timestamp to tile_index"""
    dt = datetime(2022, 1, 1)
    with pytest.raises(ValueError) as excinfo:
        date_util.timestamp_to_tile_index(dt, mock_snowflake_tile)
    assert "UTC timezone is required" in str(excinfo.value)


def test_timestamp_to_tile_index_incorrect_timezone(mock_snowflake_tile):
    """Test convert timestamp to tile_index"""
    dt = datetime(2022, 1, 1, tzinfo=timezone(timedelta(hours=8)))
    with pytest.raises(ValueError) as excinfo:
        date_util.timestamp_to_tile_index(dt, mock_snowflake_tile)
    assert "UTC timezone is required" in str(excinfo.value)


def test_tile_index_to_timestamp(mock_snowflake_tile):
    """Test convert timestamp to tile_index"""
    expected_dt = datetime(2021, 12, 31, 23, 58, tzinfo=timezone.utc)
    derived_dt = date_util.tile_index_to_timestamp(5469983, mock_snowflake_tile)
    assert derived_dt == expected_dt
