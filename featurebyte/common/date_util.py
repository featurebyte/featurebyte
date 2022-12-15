"""
Date related common utility function
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from featurebyte.models.tile import TileSpec


def timestamp_to_tile_index(input_dt: datetime, tile_spec: TileSpec) -> int:
    """
    Convert datetime to tile index

    Parameters
    ----------
    input_dt: datetime
        Input datetime
    tile_spec: TileSpec
        tile spec

    Raises
    ----------
    ValueError
        When input datetime timezone is empty or is not UTC

    Returns
    -------
    tile index
    """
    if not input_dt.tzinfo or input_dt.tzname() != "UTC":
        raise ValueError("UTC timezone is required")

    offset = tile_spec.time_modulo_frequency_second - tile_spec.blind_spot_second
    adjusted_ts = input_dt - timedelta(seconds=offset)
    period_in_seconds = (adjusted_ts - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds()
    tile_ind = int(period_in_seconds / (tile_spec.frequency_minute * 60))
    return tile_ind


def tile_index_to_timestamp(tile_index: int, tile_spec: TileSpec) -> datetime:
    """
    Convert tile index to datetime

    Parameters
    ----------
    tile_index: int
        tile index
    tile_spec: TileSpec
        tile spec

    Returns
    -------
    corresponding datetime
    """
    offset = tile_spec.time_modulo_frequency_second - tile_spec.blind_spot_second
    period_in_seconds = tile_index * tile_spec.frequency_minute * 60
    epoch_ts = datetime.fromtimestamp(period_in_seconds).astimezone(timezone.utc)
    adjusted_ts = epoch_ts + timedelta(seconds=offset)
    return adjusted_ts
