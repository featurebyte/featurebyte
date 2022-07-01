"""
Unit test for snowflake session
"""
import os
from unittest.mock import call, patch

import pandas as pd
import pytest

from featurebyte.enum import DBVarType
from featurebyte.session.snowflake import SchemaInitializer, SnowflakeSession


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


@pytest.fixture(name="patched_snowflake_session_cls")
def patched_snowflake_session_cls_fixture(schema_exists):
    """Fixture for a patched session class"""
    with patch("featurebyte.session.snowflake.SnowflakeSession", autospec=True) as patched_class:
        mock_session_obj = patched_class.return_value
        if schema_exists:
            schemas = pd.DataFrame({"name": ["PUBLIC", "FEATUREBYTE"]})
        else:
            schemas = pd.DataFrame({"name": ["PUBLIC"]})
        mock_session_obj.execute_query.side_effect = lambda _: schemas
        yield patched_class


def test_schema_initializer__sql_filenames():
    """Test retrieving SQL filenames"""
    filenames = {os.path.basename(x) for x in SchemaInitializer.get_custom_function_sql_filenames()}
    assert filenames == {
        "F_COMPUTE_TILE_INDICES.sql",
        "SP_TILE_MONITOR.sql",
        "F_TIMESTAMP_TO_INDEX.sql",
        "SP_TILE_TRIGGER_GENERATE_SCHEDULE.sql",
        "F_INDEX_TO_TIMESTAMP.sql",
        "SP_TILE_GENERATE_SCHEDULE.sql",
        "SP_TILE_GENERATE.sql",
    }


@pytest.mark.parametrize("schema_exists", [True, False])
def test_schema_initializer(patched_snowflake_session_cls, schema_exists):
    """Test SchemaInitializer executes expected queries"""
    session = patched_snowflake_session_cls()
    SchemaInitializer(session, "FEATUREBYTE").initialize()
    if schema_exists:
        # Nothing to do except checking schemas
        assert session.execute_query.call_args_list == [call("SHOW SCHEMAS")]
    else:
        # Should create schema if not exists
        assert session.execute_query.call_args_list[:2] == [
            call("SHOW SCHEMAS"),
            call("CREATE SCHEMA FEATUREBYTE"),
        ]
        # Should register custom functions and procedures
        for call_args in session.execute_query.call_args_list[2:]:
            args = call_args[0]
            assert args[0].startswith("CREATE OR REPLACE PROCEDURE") or args[0].startswith(
                "CREATE OR REPLACE FUNCTION"
            )
        assert session.execute_query.call_count > 2
