"""
Unit test for snowflake session
"""
import os
from unittest.mock import Mock, call, patch

import numpy as np
import pandas as pd
import pytest
from snowflake.connector.constants import QueryStatus
from snowflake.connector.errors import DatabaseError, NotSupportedError

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
        "sf_schema": "FEATUREBYTE",
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
@pytest.mark.asyncio
async def test_snowflake_session__credential_from_config(snowflake_session_dict):
    """
    Test snowflake session
    """
    session = SnowflakeSession(**snowflake_session_dict)
    assert session.username == "username"
    assert session.password == "password"
    assert await session.list_databases() == ["sf_database"]
    assert await session.list_schemas(database_name="sf_database") == ["sf_schema"]
    assert await session.list_tables(database_name="sf_database", schema_name="sf_schema") == [
        "sf_table",
        "sf_view",
    ]
    assert await session.list_table_schema(
        database_name="sf_database", schema_name="sf_schema", table_name="sf_table"
    ) == {
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
    assert await session.list_table_schema(
        database_name="sf_database", schema_name="sf_schema", table_name="sf_view"
    ) == {
        "col_date": DBVarType.DATE,
        "col_time": DBVarType.TIME,
        "col_timestamp_ltz": DBVarType.TIMESTAMP,
        "col_timestamp_ntz": DBVarType.TIMESTAMP,
        "col_timestamp_tz": DBVarType.TIMESTAMP,
    }


@pytest.fixture(name="mock_snowflake_cursor")
def mock_snowflake_cursor_fixture(is_fetch_pandas_all_available):
    """
    Fixture for a mocked connection cursor for Snowflake
    """
    with patch("featurebyte.session.snowflake.connector", autospec=True) as mock_connector:
        mock_cursor = Mock(name="MockCursor", description=[["col_a"], ["col_b"], ["col_c"]])
        if not is_fetch_pandas_all_available:
            mock_cursor.fetch_pandas_all.side_effect = NotSupportedError
            mock_cursor.fetchall.return_value = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        mock_connection = Mock(name="MockConnection")
        mock_connection.cursor.return_value = mock_cursor
        mock_connector.connect.return_value = mock_connection
        yield mock_cursor


@pytest.fixture(name="mock_schema_initializer")
def mock_schema_initializer_fixture():
    """Fixture to mock SchemaInitializer as no-op"""
    with patch("featurebyte.session.snowflake.SchemaInitializer") as mocked:
        yield mocked


@pytest.mark.usefixtures("mock_schema_initializer")
@pytest.mark.parametrize("is_fetch_pandas_all_available", [False, True])
@pytest.mark.asyncio
async def test_snowflake_session__fetch_pandas_all(
    snowflake_session_dict,
    mock_snowflake_cursor,
    is_fetch_pandas_all_available,
):
    """
    Test snowflake session fetch query result
    """
    session = SnowflakeSession(**snowflake_session_dict)
    result = await session.execute_query("SELECT * FROM T")
    assert mock_snowflake_cursor.fetch_pandas_all.call_count == 1
    if is_fetch_pandas_all_available:
        assert mock_snowflake_cursor.fetchall.call_count == 0
    else:
        assert mock_snowflake_cursor.fetchall.call_count == 1
        expected_result = pd.DataFrame(
            {
                "col_a": [1, 4, 7],
                "col_b": [2, 5, 8],
                "col_c": [3, 6, 9],
            }
        )
        pd.testing.assert_frame_equal(result, expected_result)


EXPECTED_FUNCTIONS = [
    "F_INDEX_TO_TIMESTAMP",
    "F_TIMESTAMP_TO_INDEX",
    "F_COUNT_DICT_ENTROPY",
    "F_COUNT_DICT_MOST_FREQUENT",
    "F_COUNT_DICT_NUM_UNIQUE",
    "F_COUNT_DICT_COSINE_SIMILARITY",
]

EXPECTED_PROCEDURES = [
    "SP_TILE_REGISTRY",
    "SP_TILE_GENERATE",
    "SP_TILE_GENERATE_SCHEDULE",
    "SP_TILE_MONITOR",
    "SP_TILE_TRIGGER_GENERATE_SCHEDULE",
    "SP_TILE_GENERATE_ENTITY_TRACKING",
]

EXPECTED_TABLES = [
    "FEATURE_LIST_REGISTRY",
    "FEATURE_REGISTRY",
    "TILE_REGISTRY",
    "TILE_MONITOR_SUMMARY",
]


@pytest.fixture(name="patched_snowflake_session_cls")
def patched_snowflake_session_cls_fixture(
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
    snowflake_session_dict_without_credentials,
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
        if not query.startswith("SHOW "):
            return None
        if query == "SHOW SCHEMAS":
            return schemas_output
        if query.startswith("SHOW USER FUNCTIONS"):
            return functions_output
        if query.startswith("SHOW PROCEDURES"):
            return procedures_output
        if query.startswith("SHOW TABLES"):
            return tables_output
        raise AssertionError(f"Unknown query: {query}")

    with patch("featurebyte.session.snowflake.SnowflakeSession", autospec=True) as patched_class:
        mock_session_obj = patched_class.return_value
        mock_session_obj.execute_query.side_effect = mock_execute_query
        mock_session_obj.database = snowflake_session_dict_without_credentials["database"]
        mock_session_obj.sf_schema = snowflake_session_dict_without_credentials["sf_schema"]
        yield patched_class


def test_schema_initializer__sql_objects():
    """Test retrieving SQL objects"""
    sql_objects = SchemaInitializer.get_sql_objects()
    for item in sql_objects:
        item["filename"] = os.path.basename(item["filename"])
        item["type"] = item["type"].value
    expected = [
        {"filename": "SP_TILE_REGISTRY.sql", "identifier": "SP_TILE_REGISTRY", "type": "procedure"},
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
            "filename": "F_INDEX_TO_TIMESTAMP.sql",
            "identifier": "F_INDEX_TO_TIMESTAMP",
            "type": "function",
        },
        {
            "filename": "F_COUNT_DICT_ENTROPY.sql",
            "identifier": "F_COUNT_DICT_ENTROPY",
            "type": "function",
        },
        {
            "filename": "F_COUNT_DICT_MOST_FREQUENT.sql",
            "identifier": "F_COUNT_DICT_MOST_FREQUENT",
            "type": "function",
        },
        {
            "filename": "F_COUNT_DICT_NUM_UNIQUE.sql",
            "identifier": "F_COUNT_DICT_NUM_UNIQUE",
            "type": "function",
        },
        {
            "filename": "F_COUNT_DICT_COSINE_SIMILARITY.sql",
            "identifier": "F_COUNT_DICT_COSINE_SIMILARITY",
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
        {
            "filename": "SP_TILE_GENERATE_ENTITY_TRACKING.sql",
            "identifier": "SP_TILE_GENERATE_ENTITY_TRACKING",
            "type": "procedure",
        },
        {
            "filename": "T_TILE_MONITOR_SUMMARY.sql",
            "identifier": "TILE_MONITOR_SUMMARY",
            "type": "table",
        },
    ]

    def _sorted_result(lst):
        return sorted(lst, key=lambda x: x["filename"])

    assert _sorted_result(sql_objects) == _sorted_result(expected)


def check_create_commands(mock_session):
    """Helper function to count the number of different create commands"""
    counts = {
        "schema": 0,
        "tables": 0,
        "functions": 0,
        "procedures": 0,
    }
    for exec_calls in [mock_session.execute_query, mock_session.execute_async_query]:
        for call_args in exec_calls.call_args_list:
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
@pytest.mark.asyncio
async def test_schema_initializer__everything_exists(
    patched_snowflake_session_cls,
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    """Test SchemaInitializer executes expected queries"""

    _ = is_schema_missing
    _ = is_functions_missing
    _ = is_procedures_missing
    _ = is_tables_missing

    session = patched_snowflake_session_cls()
    await SchemaInitializer(session).initialize()
    # Nothing to do except checking schemas and existing objects
    assert session.execute_query.call_args_list == [
        call("SHOW SCHEMAS"),
        call("SHOW USER FUNCTIONS IN DATABASE sf_database"),
        call("SHOW PROCEDURES IN DATABASE sf_database"),
        call('SHOW TABLES IN SCHEMA "sf_database"."FEATUREBYTE"'),
    ]
    counts = check_create_commands(session)
    assert counts == {"schema": 0, "functions": 0, "procedures": 0, "tables": 0}


@pytest.mark.parametrize("is_schema_missing", [True])
@pytest.mark.parametrize("is_functions_missing", [True])
@pytest.mark.parametrize("is_procedures_missing", [True])
@pytest.mark.parametrize("is_tables_missing", [True])
@pytest.mark.asyncio
async def test_schema_initializer__all_missing(
    patched_snowflake_session_cls,
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    """Test SchemaInitializer executes expected queries"""

    _ = is_schema_missing
    _ = is_functions_missing
    _ = is_procedures_missing
    _ = is_tables_missing

    session = patched_snowflake_session_cls()
    await SchemaInitializer(session).initialize()
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
@pytest.mark.asyncio
async def test_schema_initializer__partial_missing(
    patched_snowflake_session_cls,
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    """Test SchemaInitializer executes expected queries"""
    _ = is_schema_missing
    _ = is_functions_missing
    _ = is_procedures_missing
    _ = is_tables_missing

    session = patched_snowflake_session_cls()
    await SchemaInitializer(session).initialize()
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


def test_get_columns_schema_from_dataframe():
    """Test get_columns_schema_from_dataframe"""
    dataframe = pd.DataFrame(
        {
            "x_int": [1, 2, 3, 4],
            "x_float": [1.1, 2.2, 3.3, 4.4],
            "x_string": ["C1", "C2", "C3", "C4"],
            "x_date": pd.date_range("2022-01-01", periods=4),
        }
    )
    dataframe["x_int32"] = dataframe["x_int"].astype(np.int32)
    dataframe["x_int16"] = dataframe["x_int"].astype(np.int16)
    dataframe["x_int8"] = dataframe["x_int"].astype(np.int8)
    dataframe["x_float32"] = dataframe["x_float"].astype(np.float32)
    dataframe["x_float16"] = dataframe["x_float"].astype(np.float16)
    schema = SnowflakeSession.get_columns_schema_from_dataframe(dataframe)
    expected_dict = {
        "x_int": "INT",
        "x_float": "DOUBLE",
        "x_string": "VARCHAR",
        "x_date": "DATETIME",
        "x_int32": "INT",
        "x_int16": "INT",
        "x_int8": "INT",
        "x_float32": "DOUBLE",
        "x_float16": "DOUBLE",
    }
    expected_schema = ", ".join(f'"{k}" {v}' for (k, v) in expected_dict.items())
    assert schema == expected_schema


@patch("featurebyte.session.snowflake.connector")
@patch("featurebyte.session.snowflake.ASYNC_NO_DATA_MAX_RETRY", 0)
@pytest.mark.parametrize(
    "is_still_running,query_status,error_message",
    [
        (
            True,
            QueryStatus.NO_DATA,
            "Cannot retrieve data on the status of this query. No information returned from server for query 'some-query-id'",
        ),
        (
            False,
            QueryStatus.ABORTED,
            "Status of query 'some-query-id' is ABORTED, results are unavailable",
        ),
    ],
)
@pytest.mark.asyncio
async def test_snowflake_session__execute_async_query_fail(
    mock_connector,
    is_still_running,
    error_message,
    query_status,
    snowflake_session_dict,
):
    """
    Test snowflake session execute asynchronous query
    """
    connection = mock_connector.connect.return_value
    cursor = connection.cursor.return_value
    cursor.sfqid = "some-query-id"
    connection.is_still_running.return_value = is_still_running
    connection.get_query_status_throw_if_error.return_value = query_status
    session = SnowflakeSession(**snowflake_session_dict)
    with pytest.raises(DatabaseError) as exc:
        await session.execute_async_query("SELECT * FROM T")
    assert str(exc.value) == error_message
