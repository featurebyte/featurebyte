"""
Helpers for timestamp handling
"""

from __future__ import annotations

from typing import Literal

from pydantic_extra_types.timezone_name import TimeZoneName
from sqlglot import Expression

from featurebyte.query_graph.model.timestamp_schema import (
    TimestampSchema,
    TimestampTupleSchema,
    TimeZoneColumn,
    TimeZoneUnion,
)
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier


def convert_timezone(
    target_tz: Literal["utc", "local"],
    timezone_obj: TimeZoneUnion,
    adapter: BaseAdapter,
    column_expr: Expression,
) -> Expression:
    """
    Convert timestamp column to UTC

    Parameters
    ----------
    target_tz: Literal["utc", "local"]
        Target timezone
    timezone_obj: TimeZoneUnion
        Timezone information
    adapter: BaseAdapter
        SQL adapter
    column_expr: Expression
        Column expression

    Returns
    -------
    Expression
    """
    timezone_type: Literal["offset", "name"]
    if isinstance(timezone_obj, TimeZoneName):
        timezone = make_literal_value(timezone_obj)
        timezone_type = "name"
    else:
        assert isinstance(timezone_obj, TimeZoneColumn)
        timezone = quoted_identifier(timezone_obj.column_name)
        if timezone_obj.type == "offset":
            timezone_type = "offset"
        else:
            timezone_type = "name"
    if target_tz == "utc":
        return adapter.convert_timezone_to_utc(column_expr, timezone, timezone_type)
    return adapter.convert_utc_to_timezone(column_expr, timezone, timezone_type)


def convert_timestamp_to_local(
    column_expr: Expression,
    timestamp_schema: TimestampSchema,
    adapter: BaseAdapter,
) -> Expression:
    """
    Convert timestamp column in its original form with a specified TimestampSchema to local time

    Parameters
    ----------
    column_expr: Expression
        Original datetime column (could be a timestamp, date or string)
    timestamp_schema: TimestampSchema
        Timestamp schema
    adapter: BaseAdapter
        SQL adapter

    Returns
    -------
    Expression
    """
    # Convert to timestamp in local time if string
    if timestamp_schema.format_string is not None:
        column_expr = adapter.to_timestamp_from_string(column_expr, timestamp_schema.format_string)

    if timestamp_schema.is_utc_time and timestamp_schema.timezone is not None:
        # Convert to local time
        column_expr = convert_timezone(
            target_tz="local",
            timezone_obj=timestamp_schema.timezone,
            adapter=adapter,
            column_expr=column_expr,
        )

    return column_expr


def convert_timestamp_to_utc(
    column_expr: Expression,
    timestamp_schema: TimestampSchema,
    adapter: BaseAdapter,
) -> Expression:
    """
    Convert timestamp column in its original form with a specified TimestampSchema to UTC

    Parameters
    ----------
    column_expr: Expression
        Original datetime column (could be a timestamp, date or string)
    timestamp_schema: TimestampSchema
        Timestamp schema
    adapter: BaseAdapter
        SQL adapter

    Returns
    -------
    Expression
    """
    # Convert to timestamp in local time if string
    if timestamp_schema.format_string is not None:
        column_expr = adapter.to_timestamp_from_string(column_expr, timestamp_schema.format_string)

    if timestamp_schema.is_utc_time:
        # Already in UTC, nothing to do
        return column_expr

    # Convert to timestamp in UTC
    if timestamp_schema.timezone is not None:
        column_expr = convert_timezone(
            target_tz="utc",
            timezone_obj=timestamp_schema.timezone,
            adapter=adapter,
            column_expr=column_expr,
        )
    return column_expr


def convert_timestamp_timezone_tuple(
    zipped_expr: Expression,
    target_tz: Literal["utc", "local"],
    timestamp_tuple_schema: TimestampTupleSchema,
    adapter: BaseAdapter,
) -> Expression:
    """
    Extract the timestamp from a zipped timestamp and timezone offset tuple and convert it to the
    target timezone.

    Parameters
    ----------
    zipped_expr: Expression
        Zipped timestamp and timezone offset tuple
    target_tz: Literal["utc", "local"]
        Target timezone
    timestamp_tuple_schema: TimestampTupleSchema
        Timestamp tuple schema
    adapter: BaseAdapter
        SQL adapter

    Returns
    -------
    Expression
    """
    timestamp_utc_expr, timezone_offset_expr = adapter.unzip_timestamp_and_timezone(zipped_expr)
    if target_tz == "utc":
        return timestamp_utc_expr
    timezone_obj = timestamp_tuple_schema.timestamp_schema.timezone
    assert isinstance(timezone_obj, TimeZoneColumn)
    timezone_type: Literal["offset", "name"]
    if timezone_obj.type == "offset":
        timezone_type = "offset"
    else:
        timezone_type = "name"
    return adapter.convert_utc_to_timezone(timestamp_utc_expr, timezone_offset_expr, timezone_type)
