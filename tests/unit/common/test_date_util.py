"""
Tests for functions in date_util.py module
"""
from datetime import datetime, timedelta

import dateutil.parser

from featurebyte.common import date_util


def test_timestamp_to_tile_index(timestamp_to_index_fixture):
    """Test convert timestamp to tile_index"""
    (
        time_modulo_frequency_second,
        blind_spot_second,
        frequency_minute,
        time_stamp_str,
        tile_index,
    ) = timestamp_to_index_fixture

    tile_ind = date_util.timestamp_utc_to_tile_index(
        dateutil.parser.isoparse(time_stamp_str),
        time_modulo_frequency_second,
        blind_spot_second,
        frequency_minute,
    )
    assert tile_ind == tile_index


def test_tile_index_to_timestamp(index_to_timestamp_fixture):
    """Test convert timestamp to tile_index"""
    (
        tile_index,
        time_modulo_frequency_second,
        blind_spot_second,
        frequency_minute,
        time_stamp_str,
    ) = index_to_timestamp_fixture

    derived_dt = date_util.tile_index_to_timestamp_utc(
        tile_index,
        time_modulo_frequency_second,
        blind_spot_second,
        frequency_minute,
    )
    derived_dt_str = derived_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    assert derived_dt_str == time_stamp_str


def test_get_next_job_datetime():
    """Test get get next job datetime"""

    input_dt = datetime(2022, 12, 1, 12, 0, 55)
    expect_dt = datetime(2022, 12, 1, 12, 1, 20)
    next_job = date_util.get_next_job_datetime(
        input_dt, frequency_minutes=17, time_modulo_frequency_seconds=20
    )
    assert next_job == expect_dt

    input_dt = datetime(2022, 12, 1, 12, 1, 19)
    expect_dt = datetime(2022, 12, 1, 12, 1, 20)
    next_job = date_util.get_next_job_datetime(
        input_dt, frequency_minutes=17, time_modulo_frequency_seconds=20
    )
    assert next_job == expect_dt

    input_dt = datetime(2022, 12, 1, 12, 1, 20)
    expect_dt = datetime(2022, 12, 1, 12, 18, 20)
    next_job = date_util.get_next_job_datetime(
        input_dt, frequency_minutes=17, time_modulo_frequency_seconds=20
    )
    assert next_job == expect_dt
