"""
Date related common utility function
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def timestamp_utc_to_tile_index(
    input_dt: datetime,
    time_modulo_frequency_seconds: int,
    blind_spot_seconds: int,
    frequency_minute: int,
) -> int:
    """
    Convert datetime to tile index

    Parameters
    ----------
    input_dt: datetime
        Input datetime
    time_modulo_frequency_seconds: int
        time_modulo_frequency_seconds from tile spec
    blind_spot_seconds: int
        blind_spot_seconds from tile spec
    frequency_minute: int
        frequency_minute from tile spec

    Returns
    -------
    tile index
    """
    if not input_dt.tzinfo:
        input_dt = input_dt.replace(tzinfo=timezone.utc)

    if input_dt.tzname() != "UTC":
        input_dt = input_dt.astimezone(timezone.utc)

    offset = time_modulo_frequency_seconds - blind_spot_seconds
    adjusted_ts = input_dt - timedelta(seconds=offset)
    period_in_seconds = (adjusted_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    tile_ind = int(period_in_seconds / (frequency_minute * 60))
    return tile_ind


def tile_index_to_timestamp_utc(
    tile_index: int,
    time_modulo_frequency_seconds: int,
    blind_spot_seconds: int,
    frequency_minute: int,
) -> datetime:
    """
    Convert tile index to datetime

    Parameters
    ----------
    tile_index: int
        tile index
    time_modulo_frequency_seconds: int
        time_modulo_frequency_seconds from tile spec
    blind_spot_seconds: int
        blind_spot_seconds from tile spec
    frequency_minute: int
        frequency_minute from tile spec

    Returns
    -------
    corresponding datetime
    """
    offset = time_modulo_frequency_seconds - blind_spot_seconds
    period_in_seconds = tile_index * frequency_minute * 60
    epoch_ts = datetime.fromtimestamp(period_in_seconds).astimezone(timezone.utc)
    adjusted_ts = epoch_ts + timedelta(seconds=offset)
    return adjusted_ts


def get_next_job_datetime(
    input_dt: datetime, frequency_minutes: int, time_modulo_frequency_seconds: int
) -> datetime:
    """
    Calculate the next job datetime give input datetime, frequency_minutes and time_modulo_frequency_seconds

    Parameters
    ----------
    input_dt: datetime
        input datetime

    frequency_minutes: int
        frequency in minutes

    time_modulo_frequency_seconds: int
        time_modulo_frequency in seconds

    Returns
    -------
        next job time
    """
    frequency = frequency_minutes * 60

    epoch_time = datetime(1970, 1, 1)
    total_seconds = (input_dt - epoch_time).total_seconds()

    next_expected_job_seconds = (
        total_seconds // frequency
    ) * frequency + time_modulo_frequency_seconds

    if total_seconds % frequency > time_modulo_frequency_seconds:
        next_expected_job_seconds += frequency

    next_expected_job = epoch_time + timedelta(seconds=next_expected_job_seconds)

    if input_dt == next_expected_job:
        next_expected_job += timedelta(seconds=frequency)

    return next_expected_job
