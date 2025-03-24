"""
Integration tests for timestamp_handler.py
"""

import pandas as pd
import pytest
from bson import ObjectId
from sqlglot import expressions

from featurebyte.query_graph.model.timestamp_schema import TimestampSchema, TimeZoneColumn
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.timestamp_helper import (
    convert_timestamp_to_local,
    convert_timestamp_to_utc,
)


@pytest.mark.parametrize(
    "timestamp_value, timezone_offset_column, timestamp_schema, expected",
    [
        (
            pd.Timestamp("2022-01-15 10:00:00"),
            None,
            TimestampSchema(timezone="Asia/Singapore"),
            pd.Timestamp("2022-01-15 02:00:00"),
        ),
        (
            pd.Timestamp("2022-01-15 10:00:00"),
            None,
            TimestampSchema(timezone="UTC"),
            pd.Timestamp("2022-01-15 10:00:00"),
        ),
        (
            "2022|01|15",
            None,
            TimestampSchema(format_string="<date_format_placeholder>"),
            pd.Timestamp("2022-01-15 00:00:00"),
        ),
        (
            "2022|01|15",
            None,
            TimestampSchema(format_string="<date_format_placeholder>", timezone="America/New_York"),
            pd.Timestamp("2022-01-15 05:00:00"),
        ),
        (
            pd.Timestamp("2022-01-15 10:00:00"),
            {"tz_offset": "Asia/Singapore"},
            TimestampSchema(timezone=TimeZoneColumn(column_name="tz_offset", type="timezone")),
            pd.Timestamp("2022-01-15 02:00:00"),
        ),
        (
            pd.Timestamp("2022-01-15 10:00:00"),
            {"tz_offset": "+08:00"},
            TimestampSchema(timezone=TimeZoneColumn(column_name="tz_offset", type="offset")),
            pd.Timestamp("2022-01-15 02:00:00"),
        ),
    ],
)
@pytest.mark.asyncio
async def test_convert_timestamp_to_utc(
    session_without_datasets,
    timestamp_format_string,
    timestamp_value,
    timezone_offset_column,
    timestamp_schema,
    expected,
):
    """
    Test different specification of timestamp schema when constructing SCDTable
    """
    if timestamp_schema.format_string == "<date_format_placeholder>":
        timestamp_schema.format_string = timestamp_format_string
    session = session_without_datasets
    df_scd = pd.DataFrame({
        "effective_timestamp_column": pd.Series([timestamp_value]),
        "user_id": ["user_1"],
        "value": [123],
    })
    if timezone_offset_column is not None:
        assert isinstance(timezone_offset_column, dict)
        for k, v in timezone_offset_column.items():
            df_scd[k] = v
    table_name = "test_scd_view_timestamp_schema_{}".format(ObjectId()).upper()
    await session.register_table(table_name, df_scd)

    conversion_expr = expressions.select(
        expressions.alias_(
            convert_timestamp_to_utc(
                column_expr=quoted_identifier("effective_timestamp_column"),
                timestamp_schema=timestamp_schema,
                adapter=session.adapter,
            ),
            alias="result",
            quoted=True,
        )
    ).from_(quoted_identifier(table_name))
    df = await session.execute_query(sql_to_string(conversion_expr, session.source_type))
    actual = df["result"].iloc[0]
    assert actual == expected


@pytest.mark.parametrize(
    "timestamp_value, timezone_offset_column, timestamp_schema, expected",
    [
        (
            pd.Timestamp("2022-01-15 10:00:00"),
            None,
            TimestampSchema(timezone="Asia/Singapore"),
            pd.Timestamp("2022-01-15 10:00:00"),
        ),
        (
            pd.Timestamp("2022-01-15 10:00:00"),
            None,
            TimestampSchema(timezone="Asia/Singapore", is_utc_time=True),
            pd.Timestamp("2022-01-15 18:00:00"),
        ),
        (
            "2022|01|15",
            None,
            TimestampSchema(format_string="<date_format_placeholder>", timezone="America/New_York"),
            pd.Timestamp("2022-01-15 00:00:00"),
        ),
        (
            "2022|01|15",
            None,
            TimestampSchema(
                format_string="<date_format_placeholder>",
                timezone="America/New_York",
                is_utc_time=True,
            ),
            pd.Timestamp("2022-01-14 19:00:00"),
        ),
        (
            pd.Timestamp("2022-01-15 10:00:00"),
            {"tz_offset": "Asia/Singapore"},
            TimestampSchema(
                timezone=TimeZoneColumn(column_name="tz_offset", type="timezone"), is_utc_time=True
            ),
            pd.Timestamp("2022-01-15 18:00:00"),
        ),
        (
            pd.Timestamp("2022-01-15 10:00:00"),
            {"tz_offset": "+08:00"},
            TimestampSchema(
                timezone=TimeZoneColumn(column_name="tz_offset", type="offset"), is_utc_time=True
            ),
            pd.Timestamp("2022-01-15 18:00:00"),
        ),
    ],
)
@pytest.mark.asyncio
async def test_convert_timestamp_to_local(
    session_without_datasets,
    timestamp_format_string,
    timestamp_value,
    timezone_offset_column,
    timestamp_schema,
    expected,
):
    """
    Test timestamp_helper's convert_timestamp_to_local function
    """
    if timestamp_schema.format_string == "<date_format_placeholder>":
        timestamp_schema.format_string = timestamp_format_string
    session = session_without_datasets
    df_scd = pd.DataFrame({
        "effective_timestamp_column": pd.Series([timestamp_value]),
        "user_id": ["user_1"],
        "value": [123],
    })
    if timezone_offset_column is not None:
        assert isinstance(timezone_offset_column, dict)
        for k, v in timezone_offset_column.items():
            df_scd[k] = v
    table_name = "test_convert_timestamp_to_local_{}".format(ObjectId()).upper()
    await session.register_table(table_name, df_scd)

    conversion_expr = expressions.select(
        expressions.alias_(
            convert_timestamp_to_local(
                column_expr=quoted_identifier("effective_timestamp_column"),
                timestamp_schema=timestamp_schema,
                adapter=session.adapter,
            ),
            alias="result",
            quoted=True,
        )
    ).from_(quoted_identifier(table_name))
    df = await session.execute_query(sql_to_string(conversion_expr, session.source_type))
    actual = df["result"].iloc[0]
    assert actual == expected
