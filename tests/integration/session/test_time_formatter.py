"""Test time formatter"""

from datetime import datetime

import pandas as pd
import pytest
from sqlglot import expressions

from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.session.time_formatter import (
    convert_bigquery_time_format_to_python,
    convert_java_time_format_to_python,
    convert_snowflake_time_format_to_python,
)
from tests.source_types import BIGQUERY, DATABRICKS, DATABRICKS_UNITY, SNOWFLAKE, SPARK


@pytest.fixture(name="test_cases_map")
def test_cases_map_fixture():
    """Test cases map for time format conversion"""
    snowflake_test_cases = [
        ("YYYY-MM-DD", "2022-12-31"),
        ("YYYY-MON-DD", "2022-DEC-31"),
        ("YYYY-MMMM-DD", "2022-DECEMBER-31"),
        ("DY, DD-MON-YY", "SAT, 31-DEC-22"),
        ("YYYY-MM-DD HH24:MI:SS", "2022-12-31 23:59:59"),
        ("YYYY-MM-DD HH12:MI:SS AM", "2022-12-31 11:59:59 PM"),
        ("YYYY-MM-DD HH24:MI:SS.FF3", "2022-12-31 23:59:59.987"),
        ("YYYY-MM-DD HH24:MI:SS.FF6", "2022-12-31 23:59:59.123456"),
        ("YY-MM-DD HH24:MI", "22-12-31 23:59"),
        ("YYYY-MM-DDTHH24:MI:SS", "2022-12-31T23:59:59"),
        ("YYYY-MM-DD HH24:MI:SS TZHTZM", "2022-12-31 23:59:59 +0530"),
        ("YYYY-MM-DD HH24:MI:SS TZH:TZM", "2022-12-31 23:59:59 -07:00"),
    ]
    spark_test_cases = [
        ("yyyy-MM-dd", "2025-04-07"),
        ("dd/MM/yy", "07/04/25"),
        ("yyyy-MM-dd HH:mm:ss", "2025-04-07 23:45:59"),
        ("yyyy-MM-dd hh:mm:ss a", "2025-04-07 11:45:59 PM"),
        ("yyyy/MM/dd HH:mm", "2025/04/07 23:45"),
        ("d/M/yy H:m:s", "7/4/25 9:5:3"),
        ("dd MMM yyyy HH:mm:ss", "07 Apr 2025 23:45:59"),
    ]
    bigquery_test_cases = [
        ("%Y-%m-%d", "2025-04-07"),
        ("%d/%m/%y", "07/04/25"),
        ("%Y-%m-%d %H:%M:%S", "2025-04-07 23:45:59"),
        ("%Y-%m-%d %I:%M:%S %p", "2025-04-07 11:45:59 PM"),
        ("%Y/%m/%d %H:%M", "2025/04/07 23:45"),
        ("%e/%m/%y %k:%M:%S", " 7/04/25  9:45:59"),
        ("%d %b %Y %H:%M:%S", "07 Apr 2025 23:45:59"),
        ("%d %B %Y", "07 April 2025"),
        ("%Y-%m-%d %H:%M:%E*S", "2025-04-07 23:45:59.123456"),
        ("%Y-%m-%d %H:%M:%S %z", "2025-04-07 23:45:59 +0000"),
        ("%Y-%m-%d %H:%M:%S %Z", "2025-04-07 23:45:59 UTC"),
    ]
    return {
        SNOWFLAKE: snowflake_test_cases,
        DATABRICKS: spark_test_cases,
        DATABRICKS_UNITY: spark_test_cases,
        SPARK: spark_test_cases,
        BIGQUERY: bigquery_test_cases,
    }


@pytest.mark.asyncio
async def test_convert_time_format_to_python(
    config,
    session_without_datasets,
    source_type,
    test_cases_map,
):
    """Test the session initialization in snowflake works properly."""
    db_session = session_without_datasets
    adapter = db_session.adapter
    column_name = "TIME_VALUES"

    # get the time values through SQL
    union_queries = []
    for time_format, time_string in test_cases_map[source_type]:
        sql_stat = expressions.select(
            expressions.alias_(
                adapter.to_timestamp_from_string(
                    make_literal_value(time_string),
                    time_format,
                ),
                alias=column_name,
            )
        )
        union_queries.append(sql_to_string(sql_stat, source_type))

    sql_query = "\n UNION ALL \n".join(union_queries)
    results = await db_session.execute_query(sql_query)

    converter_map = {
        SNOWFLAKE: convert_snowflake_time_format_to_python,
        DATABRICKS: convert_java_time_format_to_python,
        DATABRICKS_UNITY: convert_java_time_format_to_python,
        SPARK: convert_java_time_format_to_python,
        BIGQUERY: convert_bigquery_time_format_to_python,
    }
    converter = converter_map[source_type]

    # get the time values through python
    values = []
    time_format_pairs = []
    for time_format, time_string in test_cases_map[source_type]:
        py_time_format = converter(time_format)
        dt = datetime.strptime(time_string, py_time_format)

        # ─── STRIP OFF tzinfo FOR SNOWFLAKE OFFSET TOKENS ────────────────────
        if source_type == SNOWFLAKE and "%z" in py_time_format:
            dt = dt.replace(tzinfo=None)

        values.append(dt)
        time_format_pairs.append((time_format, py_time_format))

    # for debugging only
    _ = time_format_pairs

    # compare the results
    sql_output = pd.to_datetime(results[column_name], utc=True)
    py_output = (
        pd.to_datetime(values, utc=True)
        .to_series(name=sql_output.name, index=sql_output.index)
        .astype(
            dtype=sql_output.dtype,
        )
    )
    pd.testing.assert_series_equal(sql_output, py_output, check_exact=True)
