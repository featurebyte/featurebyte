"""
Unit test for snowflake session
"""
from unittest.mock import patch

import pytest

from featurebyte.enum import DBVarType
from featurebyte.session.snowflake import SnowflakeSession


@pytest.fixture(name="os_getenv")
def mock_os_getenv():
    """
    Mock os.getenv in featurebyte.session.snowflake module
    """
    with patch("featurebyte.session.snowflake.os.getenv") as mock:
        yield mock


@pytest.fixture(name="snowflake_session_dict_without_credentials")
def snowflake_session_dict_without_credentials_fixture():
    """
    Snowflake session parameters
    """
    return {
        "account": "some_account",
        "warehouse": "some_warehouse",
        "database": "sf_database",
        "sf_schema": "sf_schema",
    }


@pytest.fixture(name="snowflake_session_dict")
def snowflake_session_dict_fixture(snowflake_session_dict_without_credentials):
    """
    Snowflake session parameters with credentials
    """
    snowflake_session_dict_without_credentials["username"] = "username"
    snowflake_session_dict_without_credentials["password"] = "password"
    return snowflake_session_dict_without_credentials


@pytest.mark.usefixtures("snowflake_connector", "snowflake_execute_query")
def test_snowflake_session__credential_from_config(snowflake_session_dict):
    """
    Test snowflake session
    """
    session = SnowflakeSession(**snowflake_session_dict)
    assert session.username == "username"
    assert session.password == "password"
    assert session.list_tables() == ["sf_table", "sf_view"]
    assert session.list_table_schema("sf_table") == {
        "col_int": DBVarType.INT,
        "col_float": DBVarType.FLOAT,
        "col_char": DBVarType.CHAR,
        "col_text": DBVarType.VARCHAR,
        "col_binary": DBVarType.BINARY,
        "col_boolean": DBVarType.BOOL,
        "created_at": DBVarType.TIMESTAMP,
        "cust_id": DBVarType.INT,
        "event_timestamp": DBVarType.TIMESTAMP,
    }
    assert session.list_table_schema("sf_view") == {
        "col_date": DBVarType.DATE,
        "col_time": DBVarType.TIME,
        "col_timestamp_ltz": DBVarType.TIMESTAMP,
        "col_timestamp_ntz": DBVarType.TIMESTAMP,
        "col_timestamp_tz": DBVarType.TIMESTAMP,
    }
