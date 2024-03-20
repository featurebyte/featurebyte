"""
Tests for featurebyte/session/session_helper.py
"""
import textwrap

import pandas as pd
import pytest

from featurebyte.models.feature_query_set import FeatureQuery, FeatureQuerySet
from featurebyte.session.session_helper import execute_feature_query_set, validate_output_row_index
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(name="is_output_row_index_valid")
def is_output_row_index_valid_fixture():
    """
    Fixture to determine whether output row index should be valid in a test
    """
    return True


@pytest.fixture(name="mock_snowflake_session")
def mock_snowflake_session_fixture(mock_snowflake_session, is_output_row_index_valid):
    """
    Mock query result
    """
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame(
        {"max_row_index_count": [1 if is_output_row_index_valid else 2]}
    )
    yield mock_snowflake_session


@pytest.mark.asyncio
async def test_validate_row_index__valid(mock_snowflake_session):
    """
    Test validate_output_row_index expected query
    """
    await validate_output_row_index(session=mock_snowflake_session, output_table_name="my_table")
    query = mock_snowflake_session.execute_query_long_running.call_args[0][0]
    expected = textwrap.dedent(
        """
        SELECT
          MAX("row_index_count") AS "max_row_index_count"
        FROM (
          SELECT
            COUNT(*) AS "row_index_count"
          FROM "my_table"
          GROUP BY
            __FB_TABLE_ROW_INDEX
        )
        """
    ).strip()
    assert query == expected


@pytest.mark.parametrize("is_output_row_index_valid", [False])
@pytest.mark.asyncio
async def test_validate_row_index__invalid(mock_snowflake_session):
    """
    Test validate_output_row_index on error case
    """
    with pytest.raises(ValueError) as exc_info:
        await validate_output_row_index(
            session=mock_snowflake_session, output_table_name="my_table"
        )
    assert (
        str(exc_info.value)
        == "Unexpected row index column in the output table. Max row index count: 2"
    )


@pytest.mark.asyncio
async def test_execute_feature_query_set(mock_snowflake_session, update_fixtures):
    """
    Test execute_feature_query_set
    """
    feature_query_set = FeatureQuerySet(
        feature_queries=[
            FeatureQuery(
                sql="CREATE TABLE my_table AS SELECT * FROM another_table",
                table_name="my_table",
                feature_names=["a"],
            ),
        ],
        output_query="CREATE TABLE output_table AS SELECT * FROM my_table",
        output_table_name="output_table",
        progress_message="Creating features",
        validate_output_row_index=True,
    )
    await execute_feature_query_set(mock_snowflake_session, feature_query_set)
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/expected_feature_query_set.sql",
        update_fixture=update_fixtures,
    )
