"""
Test base session
"""

import asyncio
import datetime
from collections import OrderedDict
from io import BytesIO
from uuid import UUID

import pandas as pd
import pyarrow as pa
import pytest
import pytest_asyncio
from bson import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte import SourceType
from featurebyte.common.utils import ARROW_METADATA_DB_VAR_TYPE
from featurebyte.exception import (
    DataWarehouseOperationError,
    QueryExecutionTimeOut,
)
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.dtype import NestedFieldMetadata
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import QueryMetadata
from tests.util.helper import truncate_timestamps


def sample_dataframe():
    """
    Test dataframe
    """
    df = pd.DataFrame({
        "index": list(range(12)),
        "bool": [False, True] * 6,
        "int": [1] * 10 + [2, 3],
        "float": [1.1] * 10 + [2.2, 3.3],
        "string": [None] * 10 + ["Bob", "Charlie"],
        "date": [datetime.date(2020, 1, 1)] * 10
        + [datetime.date(2020, 1, 2), datetime.date(2020, 1, 3)],
        "timestamp": [pd.Timestamp("2020-01-01 12:00:00")] * 10
        + [pd.Timestamp("2020-01-02 12:00:00"), pd.Timestamp("2020-01-03 12:00:00")],
        "int_list": [None] * 10 + [[4, 5, 6], [7, 8, 9]],
        "float_list": [None] * 10 + [[1.4, 2.5, 3.6], [1.7, 2.8, 3.9]],
        "string_list": [None] * 10 + [["a", "b"], ["c", "d"]],
        "binary": [None] * 10 + [b"abc", b"def"],
    })
    return truncate_timestamps(df)


def expected_dataframe():
    df = sample_dataframe()
    # create struct column from int and float columns
    df["dict"] = [
        {"a": a, "b": b} for (a, b) in list(zip(df["int"].tolist(), df["float"].tolist()))
    ]
    df["record"] = [
        {"a": a, "b": {"c": b}} for (a, b) in list(zip(df["int"].tolist(), df["float"].tolist()))
    ]
    return df


@pytest_asyncio.fixture(name="test_session", scope="session")
async def test_session_fixture(session_without_datasets):
    """
    Setup module
    """
    test_df = sample_dataframe()
    table_fqn = session_without_datasets.get_fully_qualified_table_name("TEST_DATA_TABLE")
    if session_without_datasets.source_type == SourceType.SNOWFLAKE:
        # register table does not work well with binary data for Snowflake, convert to string first
        test_df["binary"] = test_df.binary.apply(lambda x: x.decode("utf-8") if x else x)
        await session_without_datasets.register_table("TEST_DATA_TABLE", test_df)
        # add binary column separately
        await session_without_datasets.execute_query(
            f"CREATE OR REPLACE TABLE {table_fqn} AS "
            'SELECT * EXCLUDE ("binary"), '
            'TO_BINARY("binary", \'UTF-8\') AS "binary", '
            'OBJECT_CONSTRUCT(\'a\', "int", \'b\', "float") AS "dict", '
            "OBJECT_CONSTRUCT('a', \"int\", 'b', OBJECT_CONSTRUCT('c', \"float\"))::OBJECT(a INT, b OBJECT(c FLOAT)) AS \"record\" "
            f"FROM {table_fqn}"
        )
    elif session_without_datasets.source_type == SourceType.BIGQUERY:
        await session_without_datasets.register_table("TEST_DATA_TABLE", test_df)
        # register table converts binary data to string for BigQuery, convert to bytes
        await session_without_datasets.execute_query(
            f"CREATE OR REPLACE TABLE {table_fqn} AS "
            "SELECT * EXCEPT (binary), "
            "CAST(binary AS BYTES) AS binary, "
            "TO_JSON(STRUCT(int as a, float as b)) AS dict, "
            "STRUCT(int as a, STRUCT(float as c) AS b) AS record "
            f"FROM {table_fqn} ORDER BY index"
        )
    elif session_without_datasets.source_type == SourceType.SPARK:
        await session_without_datasets.register_table("TEST_DATA_TABLE", test_df)
        await session_without_datasets.execute_query(
            "CREATE OR REPLACE TABLE `TEST_DATA_TABLE` USING DELTA "
            "TBLPROPERTIES('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5') AS "
            "SELECT *, "
            "MAP('a', `int`, 'b', `float`) AS dict, "
            "STRUCT(`int` as a, STRUCT(`float` as c) as b) AS record "
            "FROM `TEST_DATA_TABLE` ORDER BY index"
        )
    else:
        await session_without_datasets.register_table("TEST_DATA_TABLE", test_df)
        await session_without_datasets.execute_query(
            f"CREATE OR REPLACE TABLE {table_fqn} AS "
            "SELECT *, "
            "MAP('a', `int`, 'b', `float`) AS dict, "
            "STRUCT(`int` as a, STRUCT(`float` as c) as b) AS record "
            f"FROM {table_fqn} ORDER BY index"
        )

    await session_without_datasets.register_table(
        table_name="job_cancel_test", dataframe=pd.DataFrame({"id": list(range(100000))})
    )
    yield session_without_datasets


@pytest.mark.asyncio
async def test_list_table_schema(test_session):
    session = test_session
    schema = await session.list_table_schema(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="TEST_DATA_TABLE",
    )
    assert schema == OrderedDict([
        (
            "index",
            ColumnSpecWithDescription(
                name="index",
                dtype="INT",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "bool",
            ColumnSpecWithDescription(
                name="bool",
                dtype="BOOL",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "int",
            ColumnSpecWithDescription(
                name="int",
                dtype="INT",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "float",
            ColumnSpecWithDescription(
                name="float",
                dtype="FLOAT",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "string",
            ColumnSpecWithDescription(
                name="string",
                dtype="VARCHAR",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "date",
            ColumnSpecWithDescription(
                name="date",
                dtype="DATE",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "timestamp",
            ColumnSpecWithDescription(
                name="timestamp",
                dtype="TIMESTAMP",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "int_list",
            ColumnSpecWithDescription(
                name="int_list",
                dtype="ARRAY",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "float_list",
            ColumnSpecWithDescription(
                name="float_list",
                dtype="ARRAY",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "string_list",
            ColumnSpecWithDescription(
                name="string_list",
                dtype="ARRAY",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "binary",
            ColumnSpecWithDescription(
                name="binary",
                dtype="BINARY",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "dict",
            ColumnSpecWithDescription(
                name="dict",
                dtype="DICT",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=None,
                description=None,
            ),
        ),
        (
            "record.a",
            ColumnSpecWithDescription(
                name="record.a",
                dtype="INT",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=NestedFieldMetadata(parent_column_name="record", keys=["a"]),
                description=None,
            ),
        ),
        (
            "record.b.c",
            ColumnSpecWithDescription(
                name="record.b.c",
                dtype="FLOAT",
                dtype_metadata=None,
                partition_metadata=None,
                nested_field_metadata=NestedFieldMetadata(
                    parent_column_name="record", keys=["b", "c"]
                ),
                description=None,
            ),
        ),
    ])


@pytest.mark.asyncio
async def test_execute_query(test_session):
    """
    Test execute query
    """
    session = test_session
    query = f"SELECT * FROM {session.get_fully_qualified_table_name('TEST_DATA_TABLE')}"
    df = await session.execute_query(query)
    expected_df = expected_dataframe()
    # expect arrays and dicts to be converted to strings
    assert_frame_equal(df, expected_df)


@pytest.mark.asyncio
async def test_fetch_empty(test_session):
    """
    Test fetch empty results
    """
    session = test_session
    query = f"SELECT * FROM {session.get_fully_qualified_table_name('TEST_DATA_TABLE')} WHERE 1 = 0"
    df = await session.execute_query(query)

    # expect empty dataframe with correct schema
    assert df.shape == (0, 13)
    assert df.dtypes.to_dict() == expected_dataframe().dtypes.to_dict()


@pytest.mark.asyncio
async def test_arrow_schema(test_session):
    """
    Test arrow schema and db variable type metadata
    """
    session = test_session
    query = f"SELECT * FROM {session.get_fully_qualified_table_name('TEST_DATA_TABLE')}"
    bytestream = session.get_async_query_stream(query)
    buffer = BytesIO()
    async for chunk in bytestream:
        buffer.write(chunk)
    buffer.seek(0)
    reader = pa.ipc.open_stream(buffer)
    arrow_table = reader.read_all()

    # expect arrays and dicts to be converted to strings
    expected_schema = pa.schema([
        pa.field("index", pa.int64()),
        pa.field("bool", pa.bool_()),
        pa.field("int", pa.int64()),
        pa.field("float", pa.float64()),
        pa.field("string", pa.string()),
        pa.field("date", pa.date64()),
        pa.field("timestamp", pa.timestamp("us", tz=None)),
        pa.field("int_list", pa.string()),
        pa.field("float_list", pa.string()),
        pa.field("string_list", pa.string()),
        pa.field("binary", pa.binary()),
        pa.field("dict", pa.string()),
        pa.field("record", pa.string()),
    ])
    assert arrow_table.schema == expected_schema

    # check db variable type metadata
    db_var_types = [
        field.metadata.get(ARROW_METADATA_DB_VAR_TYPE).decode() for field in arrow_table.schema
    ]
    assert db_var_types == [
        "INT",
        "BOOL",
        "INT",
        "FLOAT",
        "VARCHAR",
        "DATE",
        "TIMESTAMP",
        "ARRAY",
        "ARRAY",
        "ARRAY",
        "BINARY",
        "DICT",
        "DICT",
    ]


async def check_table_does_not_exist(session, name):
    """
    Helper function to check that a table or view doesn't exist
    """
    with pytest.raises(session._no_schema_error):
        await session.execute_query(
            f"SELECT * FROM {session.get_fully_qualified_table_name(name)} LIMIT 1"
        )


async def check_table_does_not_exist_or_empty(session, name):
    """
    Helper function to check that a table or view doesn't exist or is empty
    """

    try:
        df = await session.execute_query(
            f"SELECT * FROM {session.get_fully_qualified_table_name(name)} LIMIT 1"
        )
    except session._no_schema_error:
        return
    assert df.shape[0] == 0


@pytest.mark.asyncio
async def test_drop_table__table_ok(session_without_datasets):
    """
    Test drop_table function
    """
    session = session_without_datasets
    name = f"MY_TABLE_{str(ObjectId()).upper()}"
    await session.execute_query(
        f"CREATE TABLE {session.get_fully_qualified_table_name(name)} AS SELECT 1 AS A"
    )
    await session.drop_table(
        table_name=name,
        schema_name=session.schema_name,
        database_name=session.database_name,
    )
    await check_table_does_not_exist(session, name)


@pytest.mark.asyncio
async def test_drop_table__table_error(session_without_datasets):
    """
    Test drop_table function
    """
    session = session_without_datasets
    name = f"MY_TABLE_{str(ObjectId()).upper()}"
    await session.execute_query(
        f"CREATE TABLE {session.get_fully_qualified_table_name(name)} AS SELECT 1 AS A"
    )
    with pytest.raises(DataWarehouseOperationError) as exc_info:
        await session.drop_table(
            table_name="wrong_name",
            schema_name=session.schema_name,
            database_name=session.database_name,
        )
    assert "wrong_name" in str(exc_info.value)


@pytest.mark.asyncio
async def test_drop_table__view_ok(session_without_datasets):
    """
    Test drop_table function
    """
    session = session_without_datasets
    name = f"MY_VIEW_{str(ObjectId()).upper()}"
    await session.execute_query(
        f"CREATE VIEW {session.get_fully_qualified_table_name(name)} AS SELECT 1 AS A"
    )
    await session.drop_table(
        table_name=name,
        schema_name=session.schema_name,
        database_name=session.database_name,
    )
    await check_table_does_not_exist(session, name)


@pytest.mark.asyncio
async def test_drop_table__view_error(test_session):
    """
    Test drop_table function
    """
    session = test_session
    name = f"MY_VIEW_{str(ObjectId()).upper()}"
    await session.execute_query(
        f"CREATE VIEW {session.get_fully_qualified_table_name(name)} AS SELECT 1 AS A"
    )
    with pytest.raises(DataWarehouseOperationError) as exc_info:
        await session.drop_table(
            table_name="wrong_name",
            schema_name=session.schema_name,
            database_name=session.database_name,
        )
    assert "wrong_name" in str(exc_info.value)


@pytest.mark.asyncio
async def test_execute_query__error_logging(test_session, caplog):
    """Test execute query error logging"""
    session = test_session
    query = "SELECTS * FROM TEST_DATA_TABLE"
    with pytest.raises(Exception):
        await session.execute_query(query)

    record = next(
        record for record in caplog.records if record.msg.startswith("Error executing query")
    )
    assert record.extra["query"] == query, record.extra


@pytest.mark.parametrize(
    "source_type", ["spark", "databricks_unity", "snowflake", "bigquery"], indirect=True
)
@pytest.mark.asyncio
async def test_session_timeout_cancels_query(config, test_session):
    """
    Test the session timeout cancels query.
    """
    _ = config
    session = test_session
    table_name = session.get_fully_qualified_table_name("job_cancel_test")
    output_table_name = session.get_fully_qualified_table_name("job_timeout_test_output")
    with pytest.raises(QueryExecutionTimeOut):
        await session.execute_query(
            f"CREATE TABLE {output_table_name} AS SELECT A.* "
            f"FROM {table_name} AS A CROSS JOIN {table_name} AS B",
            timeout=0.1,
        )
    await check_table_does_not_exist_or_empty(session, "job_timeout_test_output")


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_task_cancellation_cancels_query(config, test_session):
    """
    Test asyncio task cancellation cancels query.
    """
    _ = config
    session = test_session
    table_name = session.get_fully_qualified_table_name("job_cancel_test")
    output_table_name = session.get_fully_qualified_table_name("job_cancel_test_output")
    with pytest.raises(asyncio.exceptions.CancelledError):
        coro = session.execute_query(
            f"CREATE TABLE {output_table_name} AS SELECT A.* "
            f"FROM {table_name} AS A CROSS JOIN {table_name} AS B",
            timeout=10,
        )
        task = asyncio.create_task(coro)
        await asyncio.sleep(1)
        task.cancel()
        await task
    await check_table_does_not_exist_or_empty(session, "job_cancel_test_output")


@pytest.mark.asyncio
async def test_timestamp_with_large_date(config, session_without_datasets):
    _ = config
    session = session_without_datasets
    query = "SELECT CAST('9999-12-31T05:00:00.123456' AS TIMESTAMP) AS TIMESTAMP"
    result = await session.execute_query(query)
    assert result.TIMESTAMP.tolist()[0] == pd.Timestamp("9999-12-31 05:00:00.123456")


@pytest.mark.asyncio
@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
async def test_timestamp_with_high_precision_date(config, session_without_datasets):
    _ = config
    session = session_without_datasets
    query = "SELECT CAST('2012-12-31T05:00:00.123456123' AS TIMESTAMP) AS TIMESTAMP"
    result = await session.execute_query(query)
    assert result.TIMESTAMP.tolist()[0] == pd.Timestamp("2012-12-31 05:00:00.123456")


@pytest.mark.parametrize(
    "source_type", ["spark", "databricks_unity", "snowflake", "bigquery"], indirect=True
)
@pytest.mark.asyncio
async def test_execute_query_long_running_with_query_id(config, test_session):
    """
    Test execute query with query ID.
    """
    _ = config
    session = test_session
    query_metadata = QueryMetadata()
    result = await session.execute_query_long_running(
        "SELECT 1 AS VALUE", query_metadata=query_metadata
    )
    assert_frame_equal(result, pd.DataFrame({"VALUE": [1]}), check_dtype=False)

    # check query ID is present and a valid UUID
    assert query_metadata.query_id is not None
    UUID(query_metadata.query_id)


@pytest.mark.parametrize(
    "source_type", ["spark", "databricks_unity", "snowflake", "bigquery"], indirect=True
)
@pytest.mark.asyncio
async def test_list_compute_options(feature_store, feature_store_credential):
    """
    Test list compute options.
    """
    compute_options = await SessionManagerService.list_compute_options(
        feature_store, feature_store_credential
    )
    if feature_store.type == SourceType.DATABRICKS_UNITY:
        assert len(compute_options) >= 1
        # All purpose clusters have a common set of details
        assert set(compute_options[0].details.keys()) == {
            "autoscale",
            "autotermination_minutes",
            "cluster_cores",
            "cluster_memory_mb",
            "creator_user_name",
            "data_security_mode",
            "driver_node_type_id",
            "node_type_id",
            "num_workers",
            "policy_id",
            "runtime_engine",
            "single_user_name",
            "spark_version",
            "state",
            "state_message",
            "workload_type",
        }
        # SQL warehouses have a different set of details
        assert set(compute_options[-1].details.keys()) == {
            "auto_stop_mins",
            "cluster_size",
            "creator_name",
            "enable_photon",
            "enable_serverless_compute",
            "max_num_clusters",
            "min_num_clusters",
            "num_clusters",
            "spot_instance_policy",
            "state",
            "warehouse_type",
        }
    elif feature_store.type == SourceType.SNOWFLAKE:
        assert len(compute_options) >= 1
        assert set(compute_options[0].details.keys()) == {
            "state",
            "type",
            "size",
            "auto_suspend",
            "auto_resume",
            "owner",
        }
    else:
        assert compute_options == []
