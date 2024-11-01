"""
Tests for models/tile_compute_query.py
"""

import textwrap

import pytest
from sqlglot import parse_one

from featurebyte.enum import SourceType
from featurebyte.models.tile_compute_query import (
    Prerequisite,
    PrerequisiteTable,
    QueryModel,
    TileComputeQuery,
)
from tests.util.helper import assert_sql_equal


@pytest.fixture(name="prerequisite_table")
def prerequisite_table_fixture():
    """
    PrerequisiteTable fixture
    """
    return PrerequisiteTable(
        name="TABLE_1",
        query=QueryModel(
            query_str="SELECT 123 AS A FROM MY_TABLE", source_type=SourceType.SNOWFLAKE
        ),
    )


@pytest.fixture(name="another_prerequisite_table")
def another_prerequisite_table_fixture():
    """
    Another PrerequisiteTable fixture
    """
    return PrerequisiteTable(
        name="TABLE_2",
        query=QueryModel(
            query_str="SELECT A + 456 AS B FROM TABLE_1", source_type=SourceType.SNOWFLAKE
        ),
    )


@pytest.fixture(name="tile_compute_query")
def tile_compute_query_fixture(prerequisite_table, another_prerequisite_table):
    """
    TileComputeQuery fixture
    """
    return TileComputeQuery(
        prerequisite=Prerequisite(
            tables=[
                prerequisite_table,
                another_prerequisite_table,
            ],
        ),
        aggregation_query=QueryModel(
            query_str="SELECT B + 789 AS OUT FROM TABLE_2", source_type=SourceType.SNOWFLAKE
        ),
    )


def test_query_model(prerequisite_table):
    """
    Test serialization of prerequisite query model
    """
    query_model = prerequisite_table.query
    assert query_model.to_expr() == parse_one("SELECT 123 AS A FROM MY_TABLE")
    assert query_model.expr is not None
    assert query_model.model_dump() == {
        "query_str": "SELECT 123 AS A FROM MY_TABLE",
        "source_type": "snowflake",
    }


def test_query_model_from_expr():
    """
    Test creation of query model from expression
    """
    query_model = QueryModel.from_expr(
        parse_one("SELECT 123 AS A FROM MY_TABLE"), source_type=SourceType.SNOWFLAKE
    )
    assert query_model.expr is not None
    assert (
        query_model.query_str
        == textwrap.dedent(
            """
        SELECT
          123 AS A
        FROM MY_TABLE
        """
        ).strip()
    )


def test_get_combined_query_expr(tile_compute_query):
    """
    Test get_combined_query_expr
    """
    assert_sql_equal(
        tile_compute_query.get_combined_query_expr().sql(pretty=True),
        """
        WITH TABLE_1 AS (
          SELECT
            123 AS A
          FROM MY_TABLE
        ), TABLE_2 AS (
          SELECT
            A + 456 AS B
          FROM TABLE_1
        )
        SELECT
          B + 789 AS OUT
        FROM TABLE_2
        """,
    )


def test_get_combined_query_string(tile_compute_query):
    """
    Test get_combined_query_string
    """
    assert_sql_equal(
        tile_compute_query.get_combined_query_string(),
        """
        WITH TABLE_1 AS (
          SELECT 123 AS A FROM MY_TABLE
        ), TABLE_2 AS (
          SELECT A + 456 AS B FROM TABLE_1
        )
        SELECT B + 789 AS OUT FROM TABLE_2
        """,
    )


def test_replace_prerequisite_table_expr(tile_compute_query):
    """
    Test replace_prerequisite_table_expr
    """
    new_tile_compute_query = tile_compute_query.replace_prerequisite_table_expr(
        "TABLE_1",
        parse_one("SELECT 12345 AS A FROM MY_TABLE"),
    )
    assert_sql_equal(
        new_tile_compute_query.get_combined_query_expr().sql(pretty=True),
        """
        WITH TABLE_1 AS (
          SELECT
            12345 AS A
          FROM MY_TABLE
        ), TABLE_2 AS (
          SELECT
            A + 456 AS B
          FROM TABLE_1
        )
        SELECT
          B + 789 AS OUT
        FROM TABLE_2
        """,
    )


def test_replace_prerequisite_table_str(tile_compute_query):
    """
    Test replace_prerequisite_table_str
    """
    new_tile_compute_query = tile_compute_query.replace_prerequisite_table_str(
        "TABLE_1",
        "SELECT 12345 AS A FROM MY_TABLE",
    )
    assert_sql_equal(
        new_tile_compute_query.get_combined_query_expr().sql(pretty=True),
        """
        WITH TABLE_1 AS (
          SELECT
            12345 AS A
          FROM MY_TABLE
        ), TABLE_2 AS (
          SELECT
            A + 456 AS B
          FROM TABLE_1
        )
        SELECT
          B + 789 AS OUT
        FROM TABLE_2
        """,
    )
