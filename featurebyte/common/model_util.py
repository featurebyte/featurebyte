"""
This module contains the implementation of feature job setting validation
"""

from __future__ import annotations

from typing import Any, Tuple

import re
from datetime import datetime

import pandas as pd
from typeguard import typechecked


def parse_duration_string(duration_string: str, minimum_seconds: int = 0) -> int:
    """Check whether the string is a valid duration

    Parameters
    ----------
    duration_string : str
        String to validate
    minimum_seconds : int
        Validate that the duration has at least this number of seconds

    Returns
    -------
    int
        Number of seconds converted from the duration string

    Raises
    ------
    ValueError
        If the specified duration is invalid
    """
    duration = pd.Timedelta(duration_string)
    duration_total_seconds = int(duration.total_seconds())
    if duration_total_seconds < minimum_seconds:
        raise ValueError(f"Duration specified is too small: {duration_string}")
    return duration_total_seconds


def validate_offset_string(offset_string: str) -> None:
    """
    Verify offset string is valid

    Parameters
    ----------
    offset_string: str
        Offset string

    Raises
    ------
    ValueError
        If the specified offset string is invalid
    """
    try:
        parse_duration_string(offset_string)
    except ValueError as exc:
        raise ValueError(
            "Failed to parse the offset parameter. An example of valid offset string is "
            f'"7d", got "{offset_string}". Error: {str(exc)}'
        ) from exc


@typechecked
def validate_job_setting_parameters(
    period: str, offset: str, blind_spot: str
) -> Tuple[int, int, int]:
    """Validate that job setting parameters are correct

    Parameters
    ----------
    period: str
        frequency of the feature job
    offset: str
        offset of when the feature job will be run, should be smaller than frequency
    blind_spot: str
        historical gap introduced to the aggregation

    Returns
    -------
    tuple
        Result of the parsed duration strings

    Raises
    ------
    ValueError
        If the specified job setting parameters are invalid
    """
    period_seconds = parse_duration_string(period, minimum_seconds=60)
    offset_seconds = parse_duration_string(offset)
    blind_spot_seconds = parse_duration_string(blind_spot)

    if offset_seconds >= period_seconds:
        raise ValueError(f"Offset ({offset}) should be smaller than period ({period})")
    return period_seconds, offset_seconds, blind_spot_seconds


def get_version() -> str:
    """
    Construct version name given feature or featurelist name

    Returns
    -------
    str
        Version name
    """
    creation_date = datetime.today().strftime("%y%m%d")
    return f"V{creation_date}"


def convert_version_string_to_dict(version: str) -> dict[str, Any]:
    """
    Convert version string to dictionary format

    Parameters
    ----------
    version: str
        Version string value

    Returns
    -------
    dict[str, Any]
    """
    name = version
    suffix = None
    if "_" in version:
        name, suffix_str = version.rsplit("_", 1)
        suffix = int(suffix_str) if suffix_str else None
    return {"name": name, "suffix": suffix}


def validate_timezone_offset_string(timezone_offset: str) -> None:
    """
    Validate timezone offset string

    Parameters
    ----------
    timezone_offset: str
        Timezone offset string

    Raises
    ------
    ValueError
        If the timezone offset string is invalid

    # noqa: DAR401
    """
    exception = ValueError(
        f"Invalid timezone_offset: {timezone_offset}. Supported format is (+/-)HH:mm, for example,"
        f"+06:00 or -03:00"
    )

    match = re.match(r"([+-])(\d\d):(\d\d)$", timezone_offset)
    if not match:
        raise exception

    _, hours, minutes = match.groups()
    hours, minutes = int(hours), int(minutes)

    if int(hours) > 24 or int(minutes) > 60:
        raise exception


def get_utc_now() -> datetime:
    """
    Get current datetime object in UTC timezone

    Returns
    -------
    datetime
        Truncated current datetime object
    """
    # exclude microseconds from timestamp as it's not supported in persistent
    utc_now = datetime.utcnow()
    utc_now = utc_now.replace(microsecond=int(utc_now.microsecond / 1000) * 1000)
    return utc_now


def convert_seconds_to_time_format(seconds: int, components: int = 4) -> str:
    """
    Convert seconds to time format

    Parameters
    ----------
    seconds: int
        Seconds to convert
    components: int
        Number of time components to include (from most to least significant)

    Returns
    -------
    str
        Time format string
    """
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    remaining_seconds = seconds % 60

    # Build the time format string
    time_format_parts = []
    if days > 0:
        time_format_parts.append(f"{days}d")
    if hours > 0:
        time_format_parts.append(f"{hours}h")
    if minutes > 0:
        time_format_parts.append(f"{minutes}m")
    if remaining_seconds > 0 or not time_format_parts:
        time_format_parts.append(f"{remaining_seconds}s")

    # Include only the most significant components as specified
    return "".join(time_format_parts[:components])
