"""
Helpers for timestamp handling
"""

from __future__ import annotations

from typing import Literal

from pydantic_extra_types.timezone_name import TimeZoneName
from sqlglot import Expression

from featurebyte.query_graph.model.timestamp_schema import (
    TimestampSchema,
    TimeZoneColumn,
    TimeZoneUnion,
)
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier


def convert_timezone_to_utc(
    timezone_obj: TimeZoneUnion, adapter: BaseAdapter, column_expr: Expression
) -> Expression:
    """
    Convert timestamp column to UTC

    Parameters
    ----------
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
    return adapter.convert_timezone_to_utc(column_expr, timezone, timezone_type)


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
    """
    # Convert to timestamp in local time if string
    if timestamp_schema.format_string is not None:
        column_expr = adapter.to_timestamp_from_string(column_expr, timestamp_schema.format_string)
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
        column_expr = convert_timezone_to_utc(
            timezone_obj=timestamp_schema.timezone,
            adapter=adapter,
            column_expr=column_expr,
        )
    return column_expr
