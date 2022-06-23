"""
Unit test for DatabaseTable
"""
import textwrap

import pandas as pd

from featurebyte.enum import DBVarType


def test_database_table(database_table):
    """
    Test DatabaseTable preview functionality
    """
    query = database_table.preview_sql()
    expected_query = textwrap.dedent(
        """
        SELECT
          "col_int",
          "col_float",
          "col_char",
          "col_text",
          "col_binary",
          "col_boolean"
        FROM "sf_table"
        LIMIT 10
        """
    ).strip()
    assert query == expected_query

    expected_dtypes = pd.Series(
        {
            "col_int": DBVarType.INT,
            "col_float": DBVarType.FLOAT,
            "col_char": DBVarType.CHAR,
            "col_text": DBVarType.VARCHAR,
            "col_binary": DBVarType.BINARY,
            "col_boolean": DBVarType.BOOL,
        }
    )
    pd.testing.assert_series_equal(database_table.dtypes, expected_dtypes)
