"""
Unit test for snowflake session
"""

import os
import time
from io import BytesIO
from unittest.mock import Mock, call, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal
from snowflake.connector.cursor import ResultMetadataV2
from snowflake.connector.errors import DatabaseError, NotSupportedError, OperationalError

from featurebyte.common.utils import dataframe_from_arrow_stream
from featurebyte.enum import DBVarType
from featurebyte.exception import DataWarehouseConnectionError, QueryExecutionTimeOut
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.table import TableDetails, TableSpec
from featurebyte.session.base import MetadataSchemaInitializer
from featurebyte.session.enum import SnowflakeDataType
from featurebyte.session.snowflake import SnowflakeSchemaInitializer, SnowflakeSession


@pytest.fixture(name="snowflake_session_dict_without_credentials")
def snowflake_session_dict_without_credentials_fixture():
    """
    Snowflake session parameters
    """
    return {
        "account": "some_account",
        "warehouse": "some_warehouse",
        "database_name": "sf_database",
        "schema_name": "FEATUREBYTE",
        "role_name": "TESTING",
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
async def test_snowflake_session__credential_from_config(
    snowflake_session_dict, snowflake_connector_cursor, snowflake_execute_query
):
    """
    Test snowflake session
    """
    session = SnowflakeSession(**snowflake_session_dict)
    await session.initialize()

    # check session initialization includes timezone and role specification
    cursor_execute_args_list = snowflake_connector_cursor.execute.call_args_list
    assert cursor_execute_args_list[0][0] == ('USE ROLE "TESTING"',)
    assert cursor_execute_args_list[1][0] == ('USE SCHEMA "sf_database"."FEATUREBYTE"',)
    assert cursor_execute_args_list[-1][0] == (
        "ALTER SESSION SET TIMEZONE='UTC', TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM'",
    )

    assert session.database_credential.username == "username"
    assert session.database_credential.password == "password"
    assert await session.list_databases() == ["sf_database"]
    assert await session.list_schemas(database_name="sf_database") == ["sf_schema"]
    tables = await session.list_tables(database_name="sf_database", schema_name="sf_schema")
    assert [table.name for table in tables] == [
        "dimension_table",
        "fixed_table",
        "items_table",
        "items_table_same_event_id",
        "non_scalar_table",
        "scd_table",
        "scd_table_state_map",
        "sf_table",
        "sf_table_5k_columns",
        "sf_table_no_tz",
        "sf_view",
    ]
    assert await session.list_table_schema(
        database_name="sf_database", schema_name="sf_schema", table_name="sf_table"
    ) == {
        "col_int": ColumnSpecWithDescription(name="col_int", dtype=DBVarType.INT),
        "col_float": ColumnSpecWithDescription(
            name="col_float", dtype=DBVarType.FLOAT, description="Float column"
        ),
        "col_char": ColumnSpecWithDescription(
            name="col_char", dtype=DBVarType.CHAR, description="Char column"
        ),
        "col_text": ColumnSpecWithDescription(
            name="col_text", dtype=DBVarType.VARCHAR, description="Text column"
        ),
        "col_binary": ColumnSpecWithDescription(name="col_binary", dtype=DBVarType.BINARY),
        "col_boolean": ColumnSpecWithDescription(name="col_boolean", dtype=DBVarType.BOOL),
        "created_at": ColumnSpecWithDescription(name="created_at", dtype=DBVarType.TIMESTAMP_TZ),
        "cust_id": ColumnSpecWithDescription(name="cust_id", dtype=DBVarType.INT),
        "event_timestamp": ColumnSpecWithDescription(
            name="event_timestamp", dtype=DBVarType.TIMESTAMP_TZ, description="Timestamp column"
        ),
    }
    assert await session.list_table_schema(
        database_name="sf_database", schema_name="sf_schema", table_name="sf_view"
    ) == {
        "col_date": ColumnSpecWithDescription(name="col_date", dtype=DBVarType.DATE),
        "col_time": ColumnSpecWithDescription(name="col_time", dtype=DBVarType.TIME),
        "col_timestamp_ltz": ColumnSpecWithDescription(
            name="col_timestamp_ltz", dtype=DBVarType.TIMESTAMP, description="Timestamp ltz column"
        ),
        "col_timestamp_ntz": ColumnSpecWithDescription(
            name="col_timestamp_ntz", dtype=DBVarType.TIMESTAMP
        ),
        "col_timestamp_tz": ColumnSpecWithDescription(
            name="col_timestamp_tz", dtype=DBVarType.TIMESTAMP_TZ, description="Timestamp tz column"
        ),
    }
    assert await session.list_table_schema(
        database_name="sf_database", schema_name="sf_schema", table_name="fixed_table"
    ) == {
        "num": ColumnSpecWithDescription(name="num", dtype=DBVarType.INT),
        "num10": ColumnSpecWithDescription(name="num10", dtype=DBVarType.FLOAT),
        "dec": ColumnSpecWithDescription(name="dec", dtype=DBVarType.FLOAT),
    }
    assert await session.list_table_schema(
        database_name="sf_database", schema_name="sf_schema", table_name="non_scalar_table"
    ) == {
        "variant": ColumnSpecWithDescription(name="variant", dtype="UNKNOWN"),
    }
    table_details = await session.get_table_details(
        database_name="sf_database", schema_name="sf_schema", table_name="sf_table"
    )
    assert table_details == TableDetails(
        details={
            "TABLE_CATALOG": "sf_database",
            "TABLE_SCHEMA": "sf_schema",
            "TABLE_NAME": "sf_table",
            "TABLE_TYPE": "VIEW",
            "COMMENT": None,
        },
        fully_qualified_name='"sf_database"."sf_schema"."sf_table"',
    )
    assert table_details.description is None


@pytest.fixture(name="mock_snowflake_cursor")
def mock_snowflake_cursor_fixture(is_fetch_pandas_all_available):
    """
    Fixture for a mocked connection cursor for Snowflake
    """
    with patch("featurebyte.session.snowflake.connector") as mock_connector:
        if not is_fetch_pandas_all_available:
            description = [["col_a"], ["col_b"], ["col_c"]]
            mock_cursor = Mock(name="MockCursor", _description=description, description=description)
            mock_cursor.fetch_pandas_all.side_effect = NotSupportedError
            mock_cursor.fetchall.return_value = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        else:
            description = [
                ResultMetadataV2(name="col_a", type_code=0, is_nullable=True),
                ResultMetadataV2(name="col_b", type_code=0, is_nullable=True),
                ResultMetadataV2(name="col_c", type_code=0, is_nullable=True),
            ]
            mock_cursor = Mock(
                name="MockCursor",
                _description=description,
                description=description,
            )
            mock_cursor.fetch_pandas_all.return_value = pd.DataFrame({
                "col_a": [1, 4, 7],
                "col_b": [2, 5, 8],
                "col_c": [3, 6, 9],
            })
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
    expected_result = pd.DataFrame({
        "col_a": [1, 4, 7],
        "col_b": [2, 5, 8],
        "col_c": [3, 6, 9],
    })
    pd.testing.assert_frame_equal(result, expected_result)


EXPECTED_FUNCTIONS = [
    "F_INDEX_TO_TIMESTAMP",
    "F_TIMESTAMP_TO_INDEX",
    "F_COUNT_DICT_ENTROPY",
    "F_COUNT_DICT_MOST_FREQUENT",
    "F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE",
    "F_COUNT_DICT_MOST_FREQUENT_VALUE",
    "F_COUNT_DICT_LEAST_FREQUENT",
    "F_COUNT_DICT_NUM_UNIQUE",
    "F_COUNT_DICT_COSINE_SIMILARITY",
    "F_GET_RANK",
    "F_GET_RELATIVE_FREQUENCY",
    "F_TIMEZONE_OFFSET_TO_SECOND",
    "F_VECTOR_AGGREGATE_MAX",
    "F_VECTOR_AGGREGATE_SUM",
    "F_VECTOR_AGGREGATE_AVG",
    "F_VECTOR_AGGREGATE_SIMPLE_AVERAGE",
    "F_VECTOR_COSINE_SIMILARITY",
]

EXPECTED_TABLES = [
    "METADATA_SCHEMA",
]

METADATA_QUERY = 'SELECT WORKING_SCHEMA_VERSION, FEATURE_STORE_ID FROM "sf_database"."FEATUREBYTE"."METADATA_SCHEMA"'


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
        functions_output = pd.DataFrame({
            "name": [],
            "schema_name": [],
        })
    else:
        functions_output = pd.DataFrame({
            "name": EXPECTED_FUNCTIONS,
            "schema_name": ["FEATUREBYTE"] * len(EXPECTED_FUNCTIONS),
        })

    if is_tables_missing:
        tables_output = pd.DataFrame({"name": [], "schema_name": []})
    else:
        tables_output = pd.DataFrame({
            "name": EXPECTED_TABLES,
            "schema_name": ["FEATUREBYTE"] * len(EXPECTED_TABLES),
        })

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
        return [TableSpec(name=name) for name in tables_output.name.tolist()]

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
        mock_session_obj.database_name = snowflake_session_dict_without_credentials["database_name"]
        mock_session_obj.schema_name = snowflake_session_dict_without_credentials["schema_name"]
        mock_session_obj.metadata_schema = '"sf_database"."FEATUREBYTE"."METADATA_SCHEMA"'
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
            "filename": "F_COUNT_DICT_LEAST_FREQUENT.sql",
            "identifier": "F_COUNT_DICT_LEAST_FREQUENT",
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
        {
            "filename": "F_VECTOR_AGGREGATE_AVG.sql",
            "identifier": "F_VECTOR_AGGREGATE_AVG",
            "type": "function",
        },
        {
            "filename": "F_VECTOR_AGGREGATE_MAX.sql",
            "identifier": "F_VECTOR_AGGREGATE_MAX",
            "type": "function",
        },
        {
            "filename": "F_VECTOR_AGGREGATE_SIMPLE_AVERAGE.sql",
            "identifier": "F_VECTOR_AGGREGATE_SIMPLE_AVERAGE",
            "type": "function",
        },
        {
            "filename": "F_VECTOR_AGGREGATE_SUM.sql",
            "identifier": "F_VECTOR_AGGREGATE_SUM",
            "type": "function",
        },
        {
            "filename": "F_VECTOR_COSINE_SIMILARITY.sql",
            "identifier": "F_VECTOR_COSINE_SIMILARITY",
            "type": "function",
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
            'CREATE TABLE IF NOT EXISTS "sf_database"."FEATUREBYTE"."METADATA_SCHEMA" '
            "( WORKING_SCHEMA_VERSION INT, MIGRATION_VERSION INT, FEATURE_STORE_ID VARCHAR, "
            "CREATED_AT TIMESTAMP DEFAULT SYSDATE() ) AS "
            "SELECT 0 AS WORKING_SCHEMA_VERSION, 3 AS MIGRATION_VERSION, NULL AS FEATURE_STORE_ID, "
            "SYSDATE() AS CREATED_AT;"
        ),
    ]
    working_schema_version = snowflake_initializer.current_working_schema_version
    assert session.execute_query.call_args_list[-1:] == [
        call(
            f'UPDATE "sf_database"."FEATUREBYTE"."METADATA_SCHEMA" SET WORKING_SCHEMA_VERSION = {working_schema_version} WHERE 1=1'
        ),
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
    dataframe = pd.DataFrame({
        "x_int": [1, 2, 3, 4],
        "x_float": [1.1, 2.2, 3.3, 4.4],
        "x_string": ["C1", "C2", "C3", "C4"],
        "x_array": [[1, 2], [3, 4], [5, 6], [7, 8]],
        "x_date": pd.date_range("2022-01-01", periods=4),
    })
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
        "x_array": "ARRAY",
        "x_date": "TIMESTAMP_NTZ",
        "x_int32": "INT",
        "x_int16": "INT",
        "x_int8": "INT",
        "x_float32": "DOUBLE",
        "x_float16": "DOUBLE",
    }
    assert schema == list(expected_dict.items())

    # test empty dataframe
    schema = SnowflakeSession.get_columns_schema_from_dataframe(dataframe.iloc[:0])
    expected_dict["x_array"] = "VARCHAR"
    assert schema == list(expected_dict.items())


@pytest.mark.parametrize("error_type", [DatabaseError, OperationalError])
def test_constructor__credentials_error(snowflake_connector, error_type, snowflake_session_dict):
    """
    Check snowflake connection exception handling
    """
    snowflake_connector.connect.side_effect = error_type(msg="Something went wrong.")
    with pytest.raises(DataWarehouseConnectionError) as exc:
        SnowflakeSession(**snowflake_session_dict)
    assert "Something went wrong." in str(exc.value)


@pytest.mark.asyncio
async def test_get_async_query_stream(snowflake_connector, snowflake_session_dict):
    """
    Test get_async_query_stream
    """
    result_data = pd.DataFrame({
        "col_a": range(10),
        "col_b": range(20, 30),
    })

    def mock_fetch_arrow_batches():
        for i in range(result_data.shape[0]):
            yield pa.Table.from_pandas(result_data.iloc[i : (i + 1)])

    connection = snowflake_connector.connect.return_value
    cursor = connection.cursor.return_value
    cursor.fetch_arrow_batches.side_effect = mock_fetch_arrow_batches
    cursor._description = cursor.description = [
        ResultMetadataV2(name="col_a", type_code=0, is_nullable=True),
        ResultMetadataV2(name="col_b", type_code=0, is_nullable=True),
    ]

    session = SnowflakeSession(**snowflake_session_dict)

    bytestream = session.get_async_query_stream("SELECT * FROM T")
    buffer = BytesIO()
    async for chunk in bytestream:
        buffer.write(chunk)
    buffer.flush()
    buffer.seek(0)
    df = dataframe_from_arrow_stream(buffer)

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
async def test_exception_handling_in_thread(snowflake_connector_cursor, snowflake_session_dict):
    """
    Test execute_query time out
    """
    session = SnowflakeSession(**snowflake_session_dict)
    snowflake_connector_cursor.execute.side_effect = ValueError("error occurred")
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
    cursor._description = cursor.description = None
    session = SnowflakeSession(**snowflake_session_dict)
    result = await session.execute_query(query)
    assert result is None
    # empty dataframe from mock_fetch_arrow_batches
    cursor._description = cursor.description = [
        ResultMetadataV2(name="a", type_code=1, is_nullable=True),
        ResultMetadataV2(name="b", type_code=1, is_nullable=True),
    ]
    empty_df = pd.DataFrame({"a": [], "b": []})

    def mock_fetch_arrow_batches():
        yield pa.Table.from_pandas(empty_df)

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
            'CREATE TABLE IF NOT EXISTS "sf_database"."FEATUREBYTE"."METADATA_SCHEMA" ( '
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
        call(
            f'UPDATE "sf_database"."FEATUREBYTE"."METADATA_SCHEMA" SET WORKING_SCHEMA_VERSION = {version_number} WHERE 1=1'
        ),
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
        call(
            'UPDATE "sf_database"."FEATUREBYTE"."METADATA_SCHEMA" SET FEATURE_STORE_ID = \'feature_store_id\' WHERE 1=1'
        ),
    ]


@pytest.mark.parametrize(
    "snowflake_var_info, expected",
    [
        ({"type": SnowflakeDataType.FIXED, "scale": 0}, DBVarType.INT),
        ({"type": SnowflakeDataType.FIXED, "scale": 2}, DBVarType.FLOAT),
        ({"type": SnowflakeDataType.REAL, "scale": 0}, DBVarType.FLOAT),
        ({"type": SnowflakeDataType.BINARY, "scale": 0}, DBVarType.BINARY),
        ({"type": SnowflakeDataType.BOOLEAN, "scale": 0}, DBVarType.BOOL),
        ({"type": SnowflakeDataType.DATE, "scale": 0}, DBVarType.DATE),
        ({"type": SnowflakeDataType.TIME, "scale": 0}, DBVarType.TIME),
        ({"type": SnowflakeDataType.TIMESTAMP_LTZ, "scale": 0}, DBVarType.TIMESTAMP),
        ({"type": SnowflakeDataType.TIMESTAMP_NTZ, "scale": 0}, DBVarType.TIMESTAMP),
        ({"type": SnowflakeDataType.TIMESTAMP_TZ, "scale": 0}, DBVarType.TIMESTAMP_TZ),
        ({"type": SnowflakeDataType.ARRAY, "scale": 0}, DBVarType.ARRAY),
        ({"type": SnowflakeDataType.OBJECT, "scale": 0}, DBVarType.DICT),
        ({"type": SnowflakeDataType.TEXT, "scale": 0, "length": 10}, DBVarType.VARCHAR),
        ({"type": SnowflakeDataType.TEXT, "scale": 0, "length": 1}, DBVarType.CHAR),
    ],
)
def test_convert_to_internal_variable_type(snowflake_var_info, expected):
    """
    Test convert_to_internal_variable_type
    """
    assert SnowflakeSession._convert_to_internal_variable_type(snowflake_var_info) == expected
