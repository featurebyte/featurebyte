"""
This module contains the implementation of feature job setting validation
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Callable, Sequence, Tuple

import pandas as pd
from pydantic import BaseModel, TypeAdapter
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


def get_type_to_class_map(
    class_list: Sequence[type[BaseModel]], discriminator_key: str = "type"
) -> dict[str, type[BaseModel]]:
    """
    Get class type string value to class map. This is used to generate the class mapping for the
    model deserialization. By using this map, we can avoid pydantic V2 performance issue due to
    _core_utils.py:walk. The issue is mentioned in https://github.com/pydantic/pydantic/issues/6768.

    Parameters
    ----------
    class_list: Sequence[type[BaseModel]]
        List of classes
    discriminator_key: str
        Discriminator key to use for mapping

    Returns
    -------
    dict[str, type[BaseModel]]
        Type to class map
    """
    class_map = {}
    for class_ in class_list:
        type_annotation = class_.model_fields[discriminator_key].annotation
        assert type_annotation is not None, class_
        type_name = type_annotation.__args__[0]
        class_map[type_name] = class_
    return class_map


def construct_serialize_function(
    all_types: Sequence[type[BaseModel]],
    annotated_type: Any,
    discriminator_key: str,
) -> Callable[..., Any]:
    """
    Construct serialize function use to serialize the object

    Parameters
    ----------
    all_types: Sequence[type[BaseModel]]
        List of all types
    annotated_type: Any
        Annotated type to use for serialization
    discriminator_key: str
        Discriminator key to use for mapping

    Returns
    -------
    Callable[..., Any]
        Function to serialize the object
    """

    # construct class map for deserialization
    class_map = get_type_to_class_map(all_types, discriminator_key=discriminator_key)

    # construct function to construct the object
    def _construct_function(**kwargs: Any) -> Any:
        specific_model_class = class_map.get(kwargs.get(discriminator_key))  # type: ignore
        if specific_model_class is None:
            # use pydantic builtin version to throw validation error (slow due to pydantic V2 performance issue)
            return TypeAdapter(annotated_type).validate_python(kwargs)

        # use internal method to avoid current pydantic V2 performance issue due to _core_utils.py:walk
        # https://github.com/pydantic/pydantic/issues/6768
        return specific_model_class(**kwargs)

    # return the constructed function
    return _construct_function
