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
    return {
        "account": "some_account",
        "warehouse": "some_warehouse",
        "database": "sf_database",
        "sf_schema": "sf_schema",
    }


@pytest.fixture(name="snowflake_session_dict")
def snowflake_session_dict_fixture(snowflake_session_dict_without_credentials):
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
    }
    assert session.list_table_schema("sf_view") == {
        "col_date": DBVarType.DATE,
        "col_time": DBVarType.TIME,
        "col_timestamp_ltz": DBVarType.TIMESTAMP,
        "col_timestamp_ntz": DBVarType.TIMESTAMP,
        "col_timestamp_tz": DBVarType.TIMESTAMP,
    }


@pytest.mark.usefixtures("snowflake_connector", "snowflake_execute_query")
def test_snowflake_session__credential_from_env(
    snowflake_session_dict_without_credentials, os_getenv
):
    """
    Test snowflake session with passing credential from environment
    """

    def _side_effect(var_name):
        env = {
            "SNOWFLAKE_USER": "snowflake_user",
            "SNOWFLAKE_PASSWORD": "snowflake_password",
        }
        return env.get(var_name)

    os_getenv.side_effect = _side_effect
    session = SnowflakeSession(**snowflake_session_dict_without_credentials)
    assert session.username == "snowflake_user"
    assert session.password == "snowflake_password"


@pytest.mark.usefixtures("snowflake_execute_query")
def test_snowflake_session__missing_credential(snowflake_session_dict_without_credentials):
    """
    Test snowflake session with missing credential
    """
    with pytest.raises(ValueError) as exc:
        SnowflakeSession(**snowflake_session_dict_without_credentials)
    assert "Username or password is empty!" in str(exc.value)
