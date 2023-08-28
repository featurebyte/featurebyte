"""
Tests for vector aggregate operations in snowflake
"""
from typing import List, Tuple

import pandas as pd
import pytest
import pytest_asyncio

from tests.util.helper import fb_assert_frame_equal


@pytest_asyncio.fixture(name="setup_test_data", scope="module")
async def setup_test_data_fixture(session):
    """
    Setup test data
    """
    # Prepare test data
    input_data = [
        (1, [4, 5, 6]),
        (1, [1, 3, 3]),
        (1, [7, 4, 9]),
        (2, [7, 8, 9]),
        (2, [4, 4, 10]),
        (2, [1, 12, 2]),
    ]
    create_table_query = (
        "CREATE OR REPLACE TABLE test_table (id_col number, array_col ARRAY, count number);"
    )
    insert_values = []
    for id_col, array_col in input_data:
        insert_values.append(f"({id_col}, '{array_col}')")
    insert_query = f"""
        INSERT INTO test_table
          SELECT $1, PARSE_JSON($2), 1
          FROM VALUES
          {", ".join(insert_values)}
          ;
        """
    queries = [create_table_query, insert_query]
    for query in queries:
        await session.execute_query(query)


def _get_query(aggregate_function: str) -> str:
    """
    Get query
    """
    return f"""
    SELECT a.id_col, b.VECTOR_AGG_RESULT AS OUT
    FROM test_table AS a, table({aggregate_function}(ARRAY_COL) OVER (partition by id_col)) AS b
    """


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_vector_aggregate_max(setup_test_data, session):
    """
    Test vector aggregate max
    """
    _ = setup_test_data
    # Run query
    query = _get_query("VECTOR_AGGREGATE_MAX")
    df = await session.execute_query(query)

    # Assert expected results
    results = [[7, 5, 9], [7, 12, 10]]
    expected_df = pd.DataFrame({"ID_COL": [1, 2], "OUT": results})
    fb_assert_frame_equal(df, expected_df, sort_by_columns=["ID_COL"])


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_vector_aggregate_sum(setup_test_data, session):
    """
    Test vector aggregate sum
    """
    _ = setup_test_data
    # Run query
    query = _get_query("VECTOR_AGGREGATE_SUM")
    df = await session.execute_query(query)

    # Assert expected results
    results = [[12, 12, 18], [12, 24, 21]]
    expected_df = pd.DataFrame({"ID_COL": [1, 2], "OUT": results})
    fb_assert_frame_equal(df, expected_df, sort_by_columns=["ID_COL"])


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_vector_aggregate_simple_avg(setup_test_data, session):
    """
    Test vector aggregate avg
    """
    _ = setup_test_data
    # Run query
    query = _get_query("VECTOR_AGGREGATE_SIMPLE_AVERAGE")
    df = await session.execute_query(query)

    # Assert expected results
    results = [[4, 4, 6], [4, 8, 7]]
    expected_df = pd.DataFrame({"ID_COL": [1, 2], "OUT": _get_formatted_output(results)})
    fb_assert_frame_equal(df, expected_df, sort_by_columns=["ID_COL"])


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_vector_aggregate_avg(setup_test_data, session):
    """
    Test vector aggregate avg
    """
    _ = setup_test_data
    # Run query
    query = """
    SELECT a.id_col, b.VECTOR_AGG_RESULT AS OUT
    FROM test_table AS a, table(VECTOR_AGGREGATE_AVG(ARRAY_COL, COUNT) OVER (partition by id_col)) AS b
    """
    df = await session.execute_query(query)

    # Assert expected results
    results = [[4, 4, 6], [4, 8, 7]]
    expected_df = pd.DataFrame({"ID_COL": [1, 2], "OUT": results})
    fb_assert_frame_equal(df, expected_df, sort_by_columns=["ID_COL"])
