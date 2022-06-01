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
    """
    Mock snowflake connector in featurebyte.session.snowflake module
    """
    with patch("featurebyte.session.snowflake.connector") as mock:
        yield mock


@pytest.fixture()
def mock_os_getenv():
    """
    Mock os.getenv in featurebyte.session.snowflake module
    """
    with patch("featurebyte.session.snowflake.os.getenv") as mock:
        yield mock


@pytest.fixture()
def mock_execute_query():
    """
    Mock execute_query in featurebyte.session.snowflake.SnowflakeSession class
    """

    def side_effect(query):
        query_map = {
            'SHOW TABLES IN SCHEMA "database"."schema"': [{"name": "table"}],
            'SHOW VIEWS IN SCHEMA "database"."schema"': [{"name": "view"}],
            'SHOW COLUMNS IN "database"."schema"."table"': [
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
            'SHOW COLUMNS IN "database"."schema"."view"': [
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
    """
    Test snowflake session when schema is set whereas database is missing
    """
    with pytest.raises(ValueError) as exc:
        SnowflakeSession(account="account", warehouse="warehouse", schema="schema")
    assert "Database name is required if schema is set" in str(exc.value)


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"SNOWFLAKE_USER": "user"},
        {"SNOWFLAKE_PASSWORD": "password"},
    ],
)
def test_snowflake_session__user_or_password_not_set(mock_os_getenv, env):
    """
    Test snowflake session when schema is set whereas database is missing
    """

    def side_effect(var_name):
        return env.get(var_name)

    mock_os_getenv.side_effect = side_effect
    with pytest.raises(ValueError) as exc:
        SnowflakeSession(account="account", warehouse="warehouse")
    expected_msg = "Environment variables 'SNOWFLAKE_USER' or 'SNOWFLAKE_PASSWORD' is not set"
    assert expected_msg in str(exc.value)


@pytest.mark.usefixtures("mock_snowflake_connector", "mock_os_getenv", "mock_execute_query")
def test_snowflake_session():
    """
    Test snowflake session
    """
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
