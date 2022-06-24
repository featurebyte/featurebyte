"""
Unit test for DatabaseTable
"""
import pandas as pd

from featurebyte.enum import DBVarType


def test_database_table(snowflake_database_table, expected_snowflake_table_preview_query):
    """
    Test DatabaseTable preview functionality
    """
    assert snowflake_database_table.preview_sql() == expected_snowflake_table_preview_query
    expected_dtypes = pd.Series(
        {
            "col_int": DBVarType.INT,
            "col_float": DBVarType.FLOAT,
            "col_char": DBVarType.CHAR,
            "col_text": DBVarType.VARCHAR,
            "col_binary": DBVarType.BINARY,
            "col_boolean": DBVarType.BOOL,
            "event_timestamp": DBVarType.TIMESTAMP,
            "created_at": DBVarType.TIMESTAMP,
            "cust_id": DBVarType.INT,
        }
    )
    pd.testing.assert_series_equal(snowflake_database_table.dtypes, expected_dtypes)
