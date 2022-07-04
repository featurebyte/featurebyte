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


EXPECTED_FUNCTIONS = ["F_COMPUTE_TILE_INDICES", "F_INDEX_TO_TIMESTAMP", "F_TIMESTAMP_TO_INDEX"]

EXPECTED_PROCEDURES = [
    "SP_TILE_GENERATE",
    "SP_TILE_GENERATE_SCHEDULE",
    "SP_TILE_MONITOR",
    "SP_TILE_TRIGGER_GENERATE_SCHEDULE",
]

EXPECTED_TABLES = [
    "FEATURE_LIST_REGISTRY",
    "FEATURE_REGISTRY",
    "TILE_REGISTRY",
]


@pytest.fixture(name="patched_snowflake_session_cls")
def patched_snowflake_session_cls_fixture(
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    """Fixture for a patched session class"""

    if is_schema_missing:
        schemas_output = pd.DataFrame({"name": ["PUBLIC"]})
    else:
        schemas_output = pd.DataFrame({"name": ["PUBLIC", "FEATUREBYTE"]})

    if is_functions_missing:
        functions_output = pd.DataFrame(
            {
                "name": [],
                "schema_name": [],
            }
        )
    else:
        functions_output = pd.DataFrame(
            {
                "name": EXPECTED_FUNCTIONS,
                "schema_name": ["FEATUREBYTE"] * len(EXPECTED_FUNCTIONS),
            }
        )

    if is_procedures_missing:
        procedures_output = pd.DataFrame({"name": [], "schema_name": []})
    else:
        procedures_output = pd.DataFrame(
            {
                "name": EXPECTED_PROCEDURES,
                "schema_name": ["FEATUREBYTE"] * len(EXPECTED_PROCEDURES),
            }
        )

    if is_tables_missing:
        tables_output = pd.DataFrame({"name": [], "schema_name": []})
    else:
        tables_output = pd.DataFrame(
            {
                "name": EXPECTED_TABLES,
                "schema_name": ["FEATUREBYTE"] * len(EXPECTED_TABLES),
            }
        )

    def mock_execute_query(query):
        if query.startswith("SHOW "):
            if query == "SHOW SCHEMAS":
                return schemas_output
            elif query.startswith("SHOW USER FUNCTIONS"):
                return functions_output
            elif query.startswith("SHOW PROCEDURES"):
                return procedures_output
            elif query.startswith("SHOW TABLES"):
                return tables_output
            raise AssertionError(f"Unknown query: {query}")

    with patch("featurebyte.session.snowflake.SnowflakeSession", autospec=True) as patched_class:
        mock_session_obj = patched_class.return_value
        mock_session_obj.execute_query.side_effect = mock_execute_query
        mock_session_obj.database = "TEST_DB"
        yield patched_class


def test_schema_initializer__sql_objects():
    """Test retrieving SQL objects"""
    sql_objects = SchemaInitializer.get_sql_objects()
    for item in sql_objects:
        item["filename"] = os.path.basename(item["filename"])
        item["type"] = item["type"].value
    expected = [
        {"filename": "SP_TILE_MONITOR.sql", "identifier": "SP_TILE_MONITOR", "type": "procedure"},
        {
            "filename": "F_TIMESTAMP_TO_INDEX.sql",
            "identifier": "F_TIMESTAMP_TO_INDEX",
            "type": "function",
        },
        {"filename": "T_FEATURE_REGISTRY.sql", "identifier": "FEATURE_REGISTRY", "type": "table"},
        {
            "filename": "SP_TILE_TRIGGER_GENERATE_SCHEDULE.sql",
            "identifier": "SP_TILE_TRIGGER_GENERATE_SCHEDULE",
            "type": "procedure",
        },
        {
            "filename": "F_COMPUTE_TILE_INDICES.sql",
            "identifier": "F_COMPUTE_TILE_INDICES",
            "type": "function",
        },
        {
            "filename": "F_INDEX_TO_TIMESTAMP.sql",
            "identifier": "F_INDEX_TO_TIMESTAMP",
            "type": "function",
        },
        {"filename": "T_TILE_REGISTRY.sql", "identifier": "TILE_REGISTRY", "type": "table"},
        {
            "filename": "SP_TILE_GENERATE_SCHEDULE.sql",
            "identifier": "SP_TILE_GENERATE_SCHEDULE",
            "type": "procedure",
        },
        {
            "filename": "T_FEATURE_LIST_REGISTRY.sql",
            "identifier": "FEATURE_LIST_REGISTRY",
            "type": "table",
        },
        {"filename": "SP_TILE_GENERATE.sql", "identifier": "SP_TILE_GENERATE", "type": "procedure"},
    ]
    assert sql_objects == expected


def check_create_commands(mock_session):
    """Helper function to count the number of different create commands"""
    counts = {
        "schema": 0,
        "tables": 0,
        "functions": 0,
        "procedures": 0,
    }
    for call_args in mock_session.execute_query.call_args_list:
        args = call_args[0]
        if args[0].startswith("CREATE SCHEMA"):
            counts["schema"] += 1
        if args[0].startswith("CREATE OR REPLACE PROCEDURE"):
            counts["procedures"] += 1
        elif args[0].startswith("CREATE OR REPLACE FUNCTION"):
            counts["functions"] += 1
        elif args[0].startswith("CREATE TABLE"):
            counts["tables"] += 1
    return counts


@pytest.mark.parametrize("is_schema_missing", [False])
@pytest.mark.parametrize("is_functions_missing", [False])
@pytest.mark.parametrize("is_procedures_missing", [False])
@pytest.mark.parametrize("is_tables_missing", [False])
def test_schema_initializer__everything_exists(
    patched_snowflake_session_cls,
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    """Test SchemaInitializer executes expected queries"""
    session = patched_snowflake_session_cls()
    SchemaInitializer(session, "FEATUREBYTE").initialize()
    # Nothing to do except checking schemas and existing objects
    assert session.execute_query.call_args_list == [
        call("SHOW SCHEMAS"),
        call("SHOW USER FUNCTIONS IN DATABASE TEST_DB"),
        call("SHOW PROCEDURES IN DATABASE TEST_DB"),
        call('SHOW TABLES IN SCHEMA "TEST_DB"."FEATUREBYTE"'),
    ]
    counts = check_create_commands(session)
    assert counts == {"schema": 0, "functions": 0, "procedures": 0, "tables": 0}


@pytest.mark.parametrize("is_schema_missing", [True])
@pytest.mark.parametrize("is_functions_missing", [True])
@pytest.mark.parametrize("is_procedures_missing", [True])
@pytest.mark.parametrize("is_tables_missing", [True])
def test_schema_initializer__all_missing(
    patched_snowflake_session_cls,
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    """Test SchemaInitializer executes expected queries"""
    session = patched_snowflake_session_cls()
    SchemaInitializer(session, "FEATUREBYTE").initialize()
    # Should create schema if not exists
    assert session.execute_query.call_args_list[:2] == [
        call("SHOW SCHEMAS"),
        call("CREATE SCHEMA FEATUREBYTE"),
    ]
    # Should register custom functions and procedures
    counts = check_create_commands(session)
    assert counts == {
        "schema": 1,
        "functions": len(EXPECTED_FUNCTIONS),
        "procedures": len(EXPECTED_PROCEDURES),
        "tables": len(EXPECTED_TABLES),
    }


@pytest.mark.parametrize("is_schema_missing", [False])
@pytest.mark.parametrize("is_functions_missing", [True, False])
@pytest.mark.parametrize("is_procedures_missing", [True, False])
@pytest.mark.parametrize("is_tables_missing", [True, False])
def test_schema_initializer__partial_missing(
    patched_snowflake_session_cls,
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    """Test SchemaInitializer executes expected queries"""
    session = patched_snowflake_session_cls()
    SchemaInitializer(session, "FEATUREBYTE").initialize()
    # Should register custom functions and procedures
    counts = check_create_commands(session)
    expected_counts = {"schema": 0, "functions": 0, "procedures": 0, "tables": 0}
    if is_functions_missing:
        expected_counts["functions"] = len(EXPECTED_FUNCTIONS)
    if is_procedures_missing:
        expected_counts["procedures"] = len(EXPECTED_PROCEDURES)
    if is_tables_missing:
        expected_counts["tables"] = len(EXPECTED_TABLES)
    assert counts == expected_counts
