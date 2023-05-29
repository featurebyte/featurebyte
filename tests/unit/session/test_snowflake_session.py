"""
Unit test for snowflake session
"""
import os
import time
from unittest.mock import Mock, call, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal
from snowflake.connector.errors import DatabaseError, NotSupportedError, OperationalError

from featurebyte.common.utils import dataframe_from_arrow_stream
from featurebyte.enum import DBVarType
from featurebyte.exception import CredentialsError, QueryExecutionTimeOut
from featurebyte.session.base import MetadataSchemaInitializer
from featurebyte.session.snowflake import SnowflakeSchemaInitializer, SnowflakeSession


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
    snowflake_session_dict_without_credentials["database_credential"] = {
        "type": "USERNAME_PASSWORD",
        "username": "username",
        "password": "password",
    }
    return snowflake_session_dict_without_credentials


@pytest.mark.usefixtures("snowflake_connector", "snowflake_execute_query")
@pytest.mark.asyncio
async def test_snowflake_session__credential_from_config(snowflake_session_dict):
    """
    Test snowflake session
    """
    session = SnowflakeSession(**snowflake_session_dict)
    assert session.database_credential.username == "username"
    assert session.database_credential.password == "password"
    assert await session.list_databases() == ["sf_database"]
    assert await session.list_schemas(database_name="sf_database") == ["sf_schema"]
    assert await session.list_tables(database_name="sf_database", schema_name="sf_schema") == [
        "sf_table",
        "sf_table_no_tz",
        "items_table",
        "items_table_same_event_id",
        "fixed_table",
        "non_scalar_table",
        "scd_table",
        "dimension_table",
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
        "created_at": DBVarType.TIMESTAMP_TZ,
        "cust_id": DBVarType.INT,
        "event_timestamp": DBVarType.TIMESTAMP_TZ,
    }
    assert await session.list_table_schema(
        database_name="sf_database", schema_name="sf_schema", table_name="sf_view"
    ) == {
        "col_date": DBVarType.DATE,
        "col_time": DBVarType.TIME,
        "col_timestamp_ltz": DBVarType.TIMESTAMP,
        "col_timestamp_ntz": DBVarType.TIMESTAMP,
        "col_timestamp_tz": DBVarType.TIMESTAMP_TZ,
    }
    assert await session.list_table_schema(
        database_name="sf_database", schema_name="sf_schema", table_name="fixed_table"
    ) == {"num": DBVarType.INT, "num10": DBVarType.FLOAT, "dec": DBVarType.FLOAT}
    assert await session.list_table_schema(
        database_name="sf_database", schema_name="sf_schema", table_name="non_scalar_table"
    ) == {"variant": "UNKNOWN"}


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
        else:
            mock_cursor.fetch_pandas_all.return_value = pd.DataFrame(
                {
                    "col_a": [1, 4, 7],
                    "col_b": [2, 5, 8],
                    "col_c": [3, 6, 9],
                }
            )
        mock_connection = Mock(name="MockConnection")
        mock_connection.cursor.return_value = mock_cursor
        mock_connector.connect.return_value = mock_connection
        # fetch_arrow_batches is disabled so we fallback to using fetch_pandas_all
        mock_cursor.fetch_arrow_batches.side_effect = NotSupportedError
        yield mock_cursor


@pytest.fixture(name="mock_schema_initializer")
def mock_schema_initializer_fixture():
    """Fixture to mock SchemaInitializer as no-op"""
    with patch("featurebyte.session.snowflake.SnowflakeSchemaInitializer") as mocked:
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
    "F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE",
    "F_COUNT_DICT_MOST_FREQUENT_VALUE",
    "F_COUNT_DICT_NUM_UNIQUE",
    "F_COUNT_DICT_COSINE_SIMILARITY",
    "F_GET_RANK",
    "F_GET_RELATIVE_FREQUENCY",
    "F_TIMEZONE_OFFSET_TO_SECOND",
]

EXPECTED_TABLES = [
    "METADATA_SCHEMA",
    "TILE_REGISTRY",
    "TILE_MONITOR_SUMMARY",
    "TILE_FEATURE_MAPPING",
    "ONLINE_STORE_MAPPING",
    "TILE_JOB_MONITOR",
]

METADATA_QUERY = "SELECT WORKING_SCHEMA_VERSION, FEATURE_STORE_ID FROM METADATA_SCHEMA"


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
        if query.startswith("SHOW USER FUNCTIONS"):
            return functions_output
        raise AssertionError(f"Unknown query: {query}")

    def mock_list_schemas(*args, **kwargs):
        _ = args
        _ = kwargs
        return schemas_output["name"].tolist()

    def mock_list_tables(*args, **kwargs):
        _ = args
        _ = kwargs
        return tables_output["name"].tolist()

    def mock_get_working_schema_metadata():
        return {
            "version": 1,
            "feature_store_id": "test_feature_store_id",
        }

    with patch("featurebyte.session.snowflake.SnowflakeSession", autospec=True) as patched_class:
        mock_session_obj = patched_class.return_value
        mock_session_obj.execute_query.side_effect = mock_execute_query
        mock_session_obj.list_schemas.side_effect = mock_list_schemas
        mock_session_obj.list_tables.side_effect = mock_list_tables
        mock_session_obj.get_working_schema_metadata.side_effect = mock_get_working_schema_metadata
        mock_session_obj.database_name = snowflake_session_dict_without_credentials["database"]
        mock_session_obj.schema_name = snowflake_session_dict_without_credentials["sf_schema"]
        yield patched_class


@pytest.mark.parametrize("is_schema_missing", [False])
@pytest.mark.parametrize("is_functions_missing", [False])
@pytest.mark.parametrize("is_procedures_missing", [False])
@pytest.mark.parametrize("is_tables_missing", [False])
def test_schema_initializer__sql_objects(
    patched_snowflake_session_cls,
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    """Test retrieving SQL objects"""
    session = patched_snowflake_session_cls()
    sql_objects = SnowflakeSchemaInitializer(session).get_sql_objects()
    for item in sql_objects:
        item["filename"] = os.path.basename(item["filename"])
        item["type"] = item["type"].value
    expected = [
        {
            "filename": "F_TIMESTAMP_TO_INDEX.sql",
            "identifier": "F_TIMESTAMP_TO_INDEX",
            "type": "function",
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
            "filename": "F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE.sql",
            "identifier": "F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE",
            "type": "function",
        },
        {
            "filename": "F_COUNT_DICT_MOST_FREQUENT_VALUE.sql",
            "identifier": "F_COUNT_DICT_MOST_FREQUENT_VALUE",
            "type": "function",
        },
        {
            "filename": "F_COUNT_DICT_NUM_UNIQUE.sql",
            "identifier": "F_COUNT_DICT_NUM_UNIQUE",
            "type": "function",
        },
        {"filename": "F_GET_RANK.sql", "identifier": "F_GET_RANK", "type": "function"},
        {
            "filename": "F_GET_RELATIVE_FREQUENCY.sql",
            "identifier": "F_GET_RELATIVE_FREQUENCY",
            "type": "function",
        },
        {
            "filename": "F_COUNT_DICT_COSINE_SIMILARITY.sql",
            "identifier": "F_COUNT_DICT_COSINE_SIMILARITY",
            "type": "function",
        },
        {
            "filename": "F_TIMEZONE_OFFSET_TO_SECOND.sql",
            "identifier": "F_TIMEZONE_OFFSET_TO_SECOND",
            "type": "function",
        },
        {"filename": "T_TILE_REGISTRY.sql", "identifier": "TILE_REGISTRY", "type": "table"},
        {
            "filename": "T_TILE_MONITOR_SUMMARY.sql",
            "identifier": "TILE_MONITOR_SUMMARY",
            "type": "table",
        },
        {
            "filename": "T_TILE_FEATURE_MAPPING.sql",
            "identifier": "TILE_FEATURE_MAPPING",
            "type": "table",
        },
        {
            "filename": "T_TILE_JOB_MONITOR.sql",
            "identifier": "TILE_JOB_MONITOR",
            "type": "table",
        },
        {
            "filename": "T_ONLINE_STORE_MAPPING.sql",
            "identifier": "ONLINE_STORE_MAPPING",
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
    }
    for call_args in mock_session.execute_query.call_args_list:
        args = call_args[0]
        if args[0].startswith("CREATE SCHEMA"):
            counts["schema"] += 1
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
async def test_schema_initializer__dont_reinitialize(
    patched_snowflake_session_cls,
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    """Test SchemaInitializer doesn't re-run all the queries when re-initialized."""

    session = patched_snowflake_session_cls()
    snowflake_initializer = SnowflakeSchemaInitializer(session)
    await snowflake_initializer.initialize()
    # Nothing to do except checking schemas and existing objects
    assert session.list_schemas.call_args_list == [call(database_name="sf_database")]
    assert session.execute_query.call_args_list[:1] == [
        call(
            "CREATE TABLE IF NOT EXISTS METADATA_SCHEMA "
            "( WORKING_SCHEMA_VERSION INT, MIGRATION_VERSION INT, FEATURE_STORE_ID VARCHAR, "
            "CREATED_AT TIMESTAMP DEFAULT SYSDATE() ) AS "
            "SELECT 0 AS WORKING_SCHEMA_VERSION, 3 AS MIGRATION_VERSION, NULL AS FEATURE_STORE_ID, "
            "SYSDATE() AS CREATED_AT;"
        ),
    ]
    working_schema_version = snowflake_initializer.current_working_schema_version
    assert session.execute_query.call_args_list[-1:] == [
        call(f"UPDATE METADATA_SCHEMA SET WORKING_SCHEMA_VERSION = {working_schema_version}"),
    ]
    counts = check_create_commands(session)
    assert counts == {
        "schema": 0,
        "functions": len(EXPECTED_FUNCTIONS),
        "tables": 1,
    }

    # update mock to have new return value for get_working_schema_metadata
    def new_mocked_get_working_schema_metadata():
        return {
            "version": working_schema_version,
            "feature_store_id": "test_feature_store_id",
        }

    session.get_working_schema_metadata.side_effect = new_mocked_get_working_schema_metadata

    existing_number_of_calls = len(session.execute_query.call_args_list)

    # re-initialize
    await snowflake_initializer.initialize()
    # verify that no additional calls to update are made
    # this is expected since we now mock out the working schema metadata to return the updated version, so
    # re-initializing should not result in anymore updates
    assert len(session.execute_query.call_args_list) == existing_number_of_calls


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
    await SnowflakeSchemaInitializer(session).initialize()
    # Should create schema if not exists
    assert session.list_schemas.call_args_list == [call(database_name="sf_database")]
    assert session.execute_query.call_args_list[:1] == [
        call('CREATE SCHEMA "FEATUREBYTE"'),
    ]
    # Should register custom functions and procedures
    counts = check_create_commands(session)
    assert counts == {
        "schema": 1,
        "functions": len(EXPECTED_FUNCTIONS),
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
    await SnowflakeSchemaInitializer(session).initialize()
    # Should register custom functions and procedures
    counts = check_create_commands(session)
    expected_counts = {
        "schema": 0,
        "functions": len(EXPECTED_FUNCTIONS),
        "tables": 1,
    }
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
        "x_date": "TIMESTAMP_NTZ",
        "x_int32": "INT",
        "x_int16": "INT",
        "x_int8": "INT",
        "x_float32": "DOUBLE",
        "x_float16": "DOUBLE",
    }
    expected_schema = list(expected_dict.items())
    assert schema == expected_schema

    # test empty dataframe
    schema = SnowflakeSession.get_columns_schema_from_dataframe(dataframe.iloc[:0])
    assert schema == expected_schema


@pytest.mark.parametrize("error_type", [DatabaseError, OperationalError])
def test_constructor__credentials_error(snowflake_connector, error_type, snowflake_session_dict):
    """
    Check snowflake connection exception handling
    """
    snowflake_connector.connect.side_effect = error_type
    with pytest.raises(CredentialsError) as exc:
        SnowflakeSession(**snowflake_session_dict)
    assert "Invalid credentials provided." in str(exc.value)


@pytest.mark.asyncio
async def test_get_async_query_stream(snowflake_connector, snowflake_session_dict):
    """
    Test get_async_query_stream
    """
    result_data = pd.DataFrame(
        {
            "col_a": range(10),
            "col_b": range(20, 30),
        }
    )

    def mock_fetch_arrow_batches():
        for i in range(result_data.shape[0]):
            yield pa.Table.from_pandas(result_data.iloc[i : (i + 1)])

    connection = snowflake_connector.connect.return_value
    cursor = connection.cursor.return_value
    cursor.fetch_arrow_batches.side_effect = mock_fetch_arrow_batches

    session = SnowflakeSession(**snowflake_session_dict)

    bytestream = session.get_async_query_stream("SELECT * FROM T")
    data = b""
    async for chunk in bytestream:
        data += chunk
    df = dataframe_from_arrow_stream(data)

    assert_frame_equal(df, result_data)


@pytest.mark.asyncio
@patch("featurebyte.session.snowflake.SnowflakeSession.fetch_query_stream_impl")
async def test_timeout(mock_fetch_query_stream_impl, snowflake_connector, snowflake_session_dict):
    """
    Test execute_query time out
    """

    def long_run(*args, **kwargs):
        """
        Simulate long execute run that exceeds timeout
        """
        time.sleep(1)

    connection = snowflake_connector.connect.return_value
    cursor = connection.cursor.return_value
    cursor.execute.side_effect = long_run
    session = SnowflakeSession(**snowflake_session_dict)

    with pytest.raises(QueryExecutionTimeOut) as exc:
        await session.execute_query("SELECT * FROM T", timeout=0.1)
    assert "Execution timeout 0.1s exceeded." in str(exc.value)

    # confirm fetch_query_stream_impl not called due to thread termination
    time.sleep(1)
    mock_fetch_query_stream_impl.assert_not_called()


@pytest.mark.asyncio
async def test_exception_handling_in_thread(snowflake_connector, snowflake_session_dict):
    """
    Test execute_query time out
    """

    connection = snowflake_connector.connect.return_value
    cursor = connection.cursor.return_value
    cursor.execute.side_effect = ValueError("error occurred")
    session = SnowflakeSession(**snowflake_session_dict)

    with pytest.raises(ValueError) as exc:
        await session.execute_query("SELECT * FROM T")
    assert "error occurred" in str(exc.value)


@pytest.mark.asyncio
async def test_execute_query_no_data(snowflake_connector, snowflake_session_dict):
    """
    Test get_async_query_stream
    """
    query = "SELECT * FROM T"
    connection = snowflake_connector.connect.return_value
    cursor = connection.cursor.return_value

    # no data, no description
    cursor.description = None
    session = SnowflakeSession(**snowflake_session_dict)
    result = await session.execute_query(query)
    assert result is None

    empty_df = pd.DataFrame({"a": [], "b": []})

    def mock_fetch_arrow_batches_empty():
        return
        yield

    # empty dataframe, no batch data from fetch_arrow_batches
    cursor.description = True
    session = SnowflakeSession(**snowflake_session_dict)
    cursor.fetch_arrow_batches.side_effect = mock_fetch_arrow_batches_empty
    cursor.get_result_batches.return_value = [Mock(to_arrow=lambda: pa.Table.from_pandas(empty_df))]
    result = await session.execute_query(query)
    assert_frame_equal(result, empty_df)

    # empty dataframe, with batch data from fetch_arrow_batches
    def mock_fetch_arrow_batches():
        yield pa.Table.from_pandas(empty_df)

    cursor.description = True
    cursor.fetch_arrow_batches.side_effect = mock_fetch_arrow_batches
    result = await session.execute_query(query)
    assert_frame_equal(result, empty_df)

    # empty dataframe, fetch_arrow_batches not supported
    cursor.fetch_arrow_batches.side_effect = NotSupportedError
    cursor.fetch_pandas_all.return_value = empty_df
    result = await session.execute_query(query)
    assert_frame_equal(result, empty_df)


@pytest.mark.parametrize("is_schema_missing", [False])
@pytest.mark.parametrize("is_functions_missing", [False])
@pytest.mark.parametrize("is_procedures_missing", [False])
@pytest.mark.parametrize("is_tables_missing", [False])
@pytest.mark.asyncio
async def test_create_metadata_table(
    patched_snowflake_session_cls,
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    session = patched_snowflake_session_cls()
    initializer = MetadataSchemaInitializer(session)
    await initializer.create_metadata_table()
    assert session.execute_query.call_args_list == [
        call(
            "CREATE TABLE IF NOT EXISTS METADATA_SCHEMA ( "
            "WORKING_SCHEMA_VERSION INT, "
            "MIGRATION_VERSION INT, "
            "FEATURE_STORE_ID VARCHAR, "
            "CREATED_AT TIMESTAMP DEFAULT SYSDATE() "
            ") AS "
            "SELECT 0 AS WORKING_SCHEMA_VERSION, "
            "3 AS MIGRATION_VERSION, "
            "NULL AS FEATURE_STORE_ID, "
            "SYSDATE() AS CREATED_AT;"
        ),
    ]


@pytest.mark.parametrize("is_schema_missing", [False])
@pytest.mark.parametrize("is_functions_missing", [False])
@pytest.mark.parametrize("is_procedures_missing", [False])
@pytest.mark.parametrize("is_tables_missing", [False])
@pytest.mark.asyncio
async def test_update_metadata_schema_version(
    patched_snowflake_session_cls,
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    session = patched_snowflake_session_cls()
    initializer = MetadataSchemaInitializer(session)
    version_number = 3
    await initializer.update_metadata_schema_version(version_number)
    assert session.execute_query.call_args_list == [
        call(f"UPDATE METADATA_SCHEMA SET WORKING_SCHEMA_VERSION = {version_number}"),
    ]


@pytest.mark.parametrize("is_schema_missing", [False])
@pytest.mark.parametrize("is_functions_missing", [False])
@pytest.mark.parametrize("is_procedures_missing", [False])
@pytest.mark.parametrize("is_tables_missing", [False])
@pytest.mark.asyncio
async def test_update_feature_store_id(
    patched_snowflake_session_cls,
    is_schema_missing,
    is_functions_missing,
    is_procedures_missing,
    is_tables_missing,
):
    session = patched_snowflake_session_cls()
    initializer = MetadataSchemaInitializer(session)
    await initializer.update_feature_store_id("feature_store_id")
    assert session.execute_query.call_args_list == [
        call("UPDATE METADATA_SCHEMA SET FEATURE_STORE_ID = 'feature_store_id'"),
    ]
