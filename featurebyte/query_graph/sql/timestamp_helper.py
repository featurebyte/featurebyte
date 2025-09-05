"""
Helpers for timestamp handling
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic_extra_types.timezone_name import TimeZoneName
from sqlglot import Expression, expressions
from sqlglot.expressions import Select

from featurebyte.enum import InternalName
from featurebyte.query_graph.model.feature_job_setting import (
    CronFeatureJobSetting,
)
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import (
    TimestampSchema,
    TimestampTupleSchema,
    TimeZoneColumn,
    TimeZoneUnion,
)
from featurebyte.query_graph.node.generic import SnapshotsDatetimeJoinKey
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.offset import OffsetDirection


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


def apply_snapshot_adjustment(
    datetime_expr: Expression,
    time_interval: TimeInterval,
    feature_job_setting: Optional[CronFeatureJobSetting],
    format_string: Optional[str],
    offset_size: Optional[int],
    adapter: BaseAdapter,
    offset_direction: OffsetDirection = OffsetDirection.BACKWARD,
) -> Expression:
    """
    Apply snapshot adjustments to a datetime expression including truncation, blind spot window,
    and formatting. This is needed to allow joining with snapshot tables via exact match on the
    adjusted datetime.

    Parameters
    ----------
    datetime_expr: Expression
        The datetime expression to adjust
    time_interval: TimeInterval
        Time interval for truncation
    feature_job_setting: Optional[CronFeatureJobSetting]
        Feature job setting containing blind spot window configuration
    format_string: Optional[str]
        Format string for timestamp formatting
    adapter: BaseAdapter
        SQL adapter

    Returns
    -------
    Expression
        Adjusted datetime expression
    """
    adjusted_datetime_expr = adapter.timestamp_truncate(
        datetime_expr,
        time_interval.unit,
    )

    if feature_job_setting is not None:
        blind_spot_window = feature_job_setting.get_blind_spot_calendar_window()
        if blind_spot_window is not None:
            if blind_spot_window.is_fixed_size():
                adjusted_datetime_expr = adapter.subtract_seconds(
                    adjusted_datetime_expr,
                    blind_spot_window.to_seconds(),
                )
            else:
                adjusted_datetime_expr = adapter.subtract_months(
                    adjusted_datetime_expr,
                    blind_spot_window.to_months(),
                )

    if offset_size is not None:
        if offset_direction == OffsetDirection.FORWARD:
            offset_size = offset_size * -1
        if time_interval.unit.is_fixed_size():
            adjusted_datetime_expr = adapter.subtract_seconds(
                adjusted_datetime_expr,
                offset_size,
            )
        else:
            adjusted_datetime_expr = adapter.subtract_months(
                adjusted_datetime_expr,
                offset_size,
            )

    if format_string is not None:
        adjusted_datetime_expr = adapter.format_timestamp(
            adjusted_datetime_expr,
            format_string,
        )

    return adjusted_datetime_expr


def get_snapshots_datetime_transform_new_column_name(
    snapshots_datetime_join_key: SnapshotsDatetimeJoinKey,
) -> str:
    """
    Get the new column name for the result of applying a snapshots datetime transform.

    Parameters
    ----------
    snapshots_datetime_join_key: SnapshotsDatetimeJoinKey
        Snapshots datetime join key

    Returns
    -------
    str
    """
    if snapshots_datetime_join_key.transform is None:
        return snapshots_datetime_join_key.column_name
    return InternalName.SNAPSHOTS_ADJUSTED_PREFIX + snapshots_datetime_join_key.column_name


def apply_snapshots_datetime_transform(
    table_expr: Select,
    snapshots_datetime_join_key: SnapshotsDatetimeJoinKey,
    adapter: BaseAdapter,
) -> Select:
    """
    Apply a SnapshotsDatetimeJoinKey by transforming the key column in the table expression and
    return a new Select expression.

    Parameters
    ----------
    table_expr: Select
        Table expression
    snapshots_datetime_join_key: SnapshotsDatetimeJoinKey
        Snapshots datetime join key
    adapter: BaseAdapter
        SQL adapter

    Returns
    -------
    Select
    """
    transform = snapshots_datetime_join_key.transform
    if transform is None:
        return table_expr

    col_expr = quoted_identifier(snapshots_datetime_join_key.column_name)
    if transform.original_timestamp_schema:
        col_timestamp_schema = transform.original_timestamp_schema
    else:
        col_timestamp_schema = TimestampSchema(is_utc_time=True)
    utc_datetime_expr = convert_timestamp_to_utc(
        column_expr=col_expr, timestamp_schema=col_timestamp_schema, adapter=adapter
    )
    if transform.snapshot_timezone_name is not None:
        snapshot_local_datetime_expr = convert_timezone(
            target_tz="local",
            timezone_obj=transform.snapshot_timezone_name,
            adapter=adapter,
            column_expr=utc_datetime_expr,
        )
    else:
        snapshot_local_datetime_expr = utc_datetime_expr
    adjusted_datetime_expr = apply_snapshot_adjustment(
        datetime_expr=snapshot_local_datetime_expr,
        time_interval=transform.snapshot_time_interval,
        feature_job_setting=transform.snapshot_feature_job_setting,
        format_string=transform.snapshot_format_string,
        offset_size=None,
        adapter=adapter,
    )

    output_expr = table_expr.select(
        expressions.alias_(
            adjusted_datetime_expr,
            alias=get_snapshots_datetime_transform_new_column_name(snapshots_datetime_join_key),
            quoted=True,
        )
    )
    return output_expr
