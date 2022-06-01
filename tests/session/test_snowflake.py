"""
Unit test for snowflake session
"""
import json
from unittest.mock import patch

import pandas as pd
import pytest

from featurebyte.enum import DBVarType
from featurebyte.session.snowflake import SnowflakeSession


@pytest.fixture()
def mock_snowflake_connector():
    with patch("featurebyte.session.snowflake.connector") as mock:
        yield mock


@pytest.fixture()
def mock_execute_query():
    def side_effect(query):
        query_map = {
            'SHOW TABLES IN SCHEMA "database"."schema"': [{"name": "table"}],
            'SHOW VIEWS IN SCHEMA "database"."schema"': [{"name": "view"}],
            'SHOW COLUMNS IN TABLE "database"."schema"."table"': [
                {"column_name": "col_int", "data_type": json.dumps({"type": "FIXED"})},
                {"column_name": "col_float", "data_type": json.dumps({"type": "REAL"})},
                {"column_name": "col_char", "data_type": json.dumps({"type": "TEXT", "length": 1})},
                {
                    "column_name": "col_text",
                    "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
                },
                {"column_name": "col_binary", "data_type": json.dumps({"type": "BINARY"})},
                {"column_name": "col_boolean", "data_type": json.dumps({"type": "BOOLEAN"})},
            ],
            'SHOW COLUMNS IN TABLE "database"."schema"."view"': [
                {"column_name": "col_date", "data_type": json.dumps({"type": "DATE"})},
                {"column_name": "col_time", "data_type": json.dumps({"type": "TIME"})},
                {
                    "column_name": "col_timestamp_ltz",
                    "data_type": json.dumps({"type": "TIMESTAMP_LTZ"}),
                },
                {
                    "column_name": "col_timestamp_ntz",
                    "data_type": json.dumps({"type": "TIMESTAMP_NTZ"}),
                },
                {
                    "column_name": "col_timestamp_tz",
                    "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                },
            ],
        }
        return pd.DataFrame(query_map[query])

    with patch("featurebyte.session.snowflake.SnowflakeSession.execute_query") as mock:
        mock.side_effect = side_effect
        yield mock


def test_snowflake_session__specified_schema_without_specified_database():
    with pytest.raises(ValueError) as exc:
        SnowflakeSession(account="account", warehouse="warehouse", schema="schema")
    assert "Database name is required if schema is set" in str(exc.value)


@pytest.mark.usefixtures("mock_snowflake_connector", "mock_execute_query")
def test_snowflake_session():
    session = SnowflakeSession(
        account="account", warehouse="warehouse", database="database", schema="schema"
    )
    assert session.database_metadata == {
        '"database"."schema"."table"': {
            "col_int": DBVarType.INT,
            "col_float": DBVarType.FLOAT,
            "col_char": DBVarType.CHAR,
            "col_text": DBVarType.VARCHAR,
            "col_binary": DBVarType.BINARY,
            "col_boolean": DBVarType.BOOL,
        },
        '"database"."schema"."view"': {
            "col_date": DBVarType.DATE,
            "col_time": DBVarType.TIME,
            "col_timestamp_ltz": DBVarType.TIMESTAMP,
            "col_timestamp_ntz": DBVarType.TIMESTAMP,
            "col_timestamp_tz": DBVarType.TIMESTAMP,
        },
    }
