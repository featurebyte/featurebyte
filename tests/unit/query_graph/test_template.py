import textwrap

import pytest
from sqlglot import parse_one

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.template import SqlExpressionTemplate


@pytest.fixture(name="sql_template")
def sql_template_fixture():
    """
    Fixture for a SqlExpressionTemplate
    """
    expr = parse_one("SELECT * FROM TABLE_A WHERE A == __PLACEHOLDER")
    return SqlExpressionTemplate(expr, SourceType.SNOWFLAKE)


def test_replace_placeholder__str(sql_template):
    """
    Test replacing placeholder with string literal
    """
    result = sql_template.render({"__PLACEHOLDER": "some_str_value"})
    assert (
        result
        == textwrap.dedent(
            """
        SELECT
          *
        FROM TABLE_A
        WHERE
          A = 'some_str_value'
        """
        ).strip()
    )


def test_replace_placeholder__number(sql_template):
    """
    Test replacing placeholder with number literal
    """
    result = sql_template.render({"__PLACEHOLDER": 123})
    assert (
        result
        == textwrap.dedent(
            """
        SELECT
          *
        FROM TABLE_A
        WHERE
          A = 123
        """
        ).strip()
    )


def test_replace_placeholder__expr(sql_template):
    """
    Test replacing placeholder with already constructed Expression
    """
    data = {"__PLACEHOLDER": 123, "TABLE_A": parse_one("SELECT * FROM ANOTHER_TABLE").subquery()}
    result = sql_template.render(data)
    assert (
        result
        == textwrap.dedent(
            """
        SELECT
          *
        FROM (
          SELECT
            *
          FROM ANOTHER_TABLE
        )
        WHERE
          A = 123
        """
        ).strip()
    )
