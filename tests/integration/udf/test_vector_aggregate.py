"""
Tests for vector aggregate operations in snowflake
"""

import pandas as pd
import pytest
import pytest_asyncio
from sqlglot import expressions

from featurebyte.enum import DBVarType
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.session.base import BaseSession
from tests.util.helper import fb_assert_frame_equal


@pytest_asyncio.fixture(name="setup_test_data", scope="module")
async def setup_test_data_fixture(session: BaseSession, to_array):
    """
    Setup test data
    """
    # Prepare test data
    input_data = [
        (1, [4, 5, 6.0]),
        (1, [1, 3, 3.0]),
        (1, [7, 4, 9.0]),
        (2, [7, 8, 9.0]),
        (2, [4, 4, 10.0]),
        (2, [1, 12, 2.0]),
    ]
    create_table_query = "CREATE TABLE TEST_TABLE_VECTOR_AGGREGATE AS\n"
    row_statements = []
    for row in input_data:
        double_type = session.adapter.get_physical_type_from_dtype(DBVarType.FLOAT)
        row_statements.append(
            f"SELECT {row[0]} AS ID_COL, {to_array(tuple(row[1]))} AS ARRAY_COL, CAST(1.0 AS {double_type}) AS COUNT"
        )
    create_table_query += "\nUNION ALL\n".join(row_statements)
    await session.execute_query(create_table_query)


def _get_query_snowflake(aggregate_function: str, with_count: bool) -> str:
    """
    Get query with Snowflake table function
    """
    args = ["ARRAY_COL"]
    if with_count:
        args.append("COUNT")
    args_str = ", ".join(args)
    return f"""
    SELECT a.ID_COL, b.VECTOR_AGG_RESULT AS OUT
    FROM TEST_TABLE_VECTOR_AGGREGATE AS a, table({aggregate_function}({args_str}) OVER (partition by ID_COL)) AS b
    """


def _get_query_groupby(session: BaseSession, aggregate_function: str, with_count: bool) -> str:
    """
    Get query with groupby and then calling vector aggregate function
    """
    udf_args = [expressions.Identifier(this="ARRAY_COL")]
    if with_count:
        udf_args.append(expressions.Identifier(this="COUNT"))
    udf_result = sql_to_string(
        session.adapter.call_vector_aggregation_function(aggregate_function, udf_args),
        source_type=session.source_type,
    )
    return f"""
    SELECT ID_COL, {udf_result} AS OUT
    FROM TEST_TABLE_VECTOR_AGGREGATE
    GROUP BY ID_COL
    """


def _get_query(session, aggregate_function: str, with_count: bool = False) -> str:
    """
    Get query
    """
    if session.source_type == "snowflake":
        return _get_query_snowflake(aggregate_function, with_count)
    return _get_query_groupby(session, aggregate_function, with_count)


@pytest.mark.asyncio
async def test_vector_aggregate_max(setup_test_data, session):
    """
    Test vector aggregate max
    """
    _ = setup_test_data
    # Run query
    query = _get_query(session, "VECTOR_AGGREGATE_MAX")
    df = await session.execute_query(query)

    # Assert expected results
    results = [[7, 5, 9], [7, 12, 10]]
    expected_df = pd.DataFrame({"ID_COL": [1, 2], "OUT": results})
    fb_assert_frame_equal(df, expected_df, sort_by_columns=["ID_COL"])


@pytest.mark.asyncio
async def test_vector_aggregate_sum(setup_test_data, session):
    """
    Test vector aggregate sum
    """
    _ = setup_test_data
    # Run query
    query = _get_query(session, "VECTOR_AGGREGATE_SUM")
    df = await session.execute_query(query)

    # Assert expected results
    results = [[12, 12, 18], [12, 24, 21]]
    expected_df = pd.DataFrame({"ID_COL": [1, 2], "OUT": results})
    fb_assert_frame_equal(df, expected_df, sort_by_columns=["ID_COL"])


@pytest.mark.asyncio
async def test_vector_aggregate_simple_avg(setup_test_data, session):
    """
    Test vector aggregate avg
    """
    _ = setup_test_data
    # Run query
    query = _get_query(session, "VECTOR_AGGREGATE_SIMPLE_AVERAGE")
    df = await session.execute_query(query)

    # Assert expected results
    results = [[4, 4, 6], [4, 8, 7]]
    expected_df = pd.DataFrame({"ID_COL": [1, 2], "OUT": results})
    fb_assert_frame_equal(df, expected_df, sort_by_columns=["ID_COL"])


@pytest.mark.asyncio
async def test_vector_aggregate_avg(setup_test_data, session):
    """
    Test vector aggregate avg
    """
    _ = setup_test_data
    # Run query
    query = _get_query(session, "VECTOR_AGGREGATE_AVG", with_count=True)
    df = await session.execute_query(query)

    # Assert expected results
    results = [[4, 4, 6], [4, 8, 7]]
    expected_df = pd.DataFrame({"ID_COL": [1, 2], "OUT": results})
    fb_assert_frame_equal(df, expected_df, sort_by_columns=["ID_COL"])
