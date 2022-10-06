"""
Tests for featurebyte.query_graph.sql.dataframe
"""
import textwrap

import numpy as np
import pandas as pd

from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr


def test_construct_dataframe_sql_expr_one_row():
    df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
    expr = construct_dataframe_sql_expr(df, [])
    expr_text = expr.sql(pretty=True)
    expected_sql = textwrap.dedent(
        """
        SELECT
          1 AS "a",
          2 AS "b",
          3 AS "c"
        """
    ).strip()
    assert expr_text == expected_sql


def test_construct_dataframe_sql_expr_multiple_rows():
    df = pd.DataFrame(
        {"a": [1, 100], "b": [2, 200], "c": [3, 300], "dt": ["2022-01-01", "2022-02-01"]}
    )
    expr = construct_dataframe_sql_expr(df, [])
    expr_text = expr.sql(pretty=True)
    expected_sql = textwrap.dedent(
        """
        SELECT
          1 AS "a",
          2 AS "b",
          3 AS "c",
          '2022-01-01' AS "dt"
        UNION ALL
        SELECT
          100 AS "a",
          200 AS "b",
          300 AS "c",
          '2022-02-01' AS "dt"
        """
    ).strip()
    assert expr_text == expected_sql


def test_construct_dataframe_sql_expr_different_types():
    df = pd.DataFrame(
        {"a": ["V1", "V2"], "b": [2, np.nan], "c": [3, 300], "dt": ["2022-01-01", None]}
    )
    expr = construct_dataframe_sql_expr(df, ["dt"])
    expr_text = expr.sql(pretty=True)
    expected_sql = textwrap.dedent(
        """
        SELECT
          'V1' AS "a",
          2.0 AS "b",
          3 AS "c",
          CAST('2022-01-01' AS TIMESTAMP) AS "dt"
        UNION ALL
        SELECT
          'V2' AS "a",
          NULL AS "b",
          300 AS "c",
          NULL AS "dt"
        """
    ).strip()
    assert expr_text == expected_sql
