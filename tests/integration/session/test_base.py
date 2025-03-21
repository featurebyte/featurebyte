"""
Test base session
"""

import asyncio
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pytest
import pytest_asyncio
from bson import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte.common.utils import ARROW_METADATA_DB_VAR_TYPE
from featurebyte.exception import (
    DataWarehouseOperationError,
    QueryExecutionTimeOut,
)


def sample_dataframe():
    """
    Test dataframe
    """
    df = pd.DataFrame({
        "bool": [False, True] * 6,
        "int": [1] * 10 + [2, 3],
        "float": [1.1] * 10 + [2.2, 3.3],
        "string": [None] * 10 + ["Bob", "Charlie"],
        "date": [pd.Timestamp("2020-01-01")] * 10
        + [pd.Timestamp("2020-01-02"), pd.Timestamp("2020-01-03")],
        "timestamp": [pd.Timestamp("2020-01-01 12:00:00")] * 10
        + [pd.Timestamp("2020-01-02 12:00:00"), pd.Timestamp("2020-01-03 12:00:00")],
        "int_list": [None] * 10 + [[4, 5, 6], [7, 8, 9]],
        "float_list": [None] * 10 + [[1.4, 2.5, 3.6], [1.7, 2.8, 3.9]],
        "string_list": [None] * 10 + [["a", "b"], ["c", "d"]],
        "dict": [None] * 10 + [{"x": 3, "y": 4}, {"x": 5, "y": 6}],
    })
    for column in df.columns:
        if df[column].dtype.name == "datetime64[ns]":
            df[column] = df[column].astype("datetime64[us]")
    return df


@pytest_asyncio.fixture(name="test_session", scope="session")
async def test_session_fixture(session_without_datasets):
    """
    Setup module
    """
    await session_without_datasets.register_table("TEST_DATA_TABLE", sample_dataframe())
    await session_without_datasets.register_table(
        table_name="job_cancel_test", dataframe=pd.DataFrame({"id": list(range(10000))})
    )
    yield session_without_datasets


@pytest.mark.asyncio
async def test_execute_query(test_session):
    """
    Test execute query
    """
    session = test_session
    query = f"SELECT * FROM {session.get_fully_qualified_table_name('TEST_DATA_TABLE')}"
    df = await session.execute_query(query)
    expected_df = sample_dataframe()
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
    assert df.shape == (0, 10)
    assert df.dtypes.to_dict() == sample_dataframe().dtypes.to_dict()


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
        pa.field("bool", pa.bool_()),
        pa.field("int", pa.int64()),
        pa.field("float", pa.float64()),
        pa.field("string", pa.string()),
        pa.field("date", pa.timestamp("us", tz=None)),
        pa.field("timestamp", pa.timestamp("us", tz=None)),
        pa.field("int_list", pa.string()),
        pa.field("float_list", pa.string()),
        pa.field("string_list", pa.string()),
        pa.field("dict", pa.string()),
    ])
    assert arrow_table.schema == expected_schema

    # check db variable type metadata
    db_var_types = [
        field.metadata.get(ARROW_METADATA_DB_VAR_TYPE).decode() for field in arrow_table.schema
    ]
    assert db_var_types == [
        "BOOL",
        "INT",
        "FLOAT",
        "VARCHAR",
        "TIMESTAMP",
        "TIMESTAMP",
        "ARRAY",
        "ARRAY",
        "ARRAY",
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


@pytest.mark.parametrize(
    "source_type", ["spark", "databricks_unity", "snowflake", "bigquery"], indirect=True
)
@pytest.mark.asyncio
async def test_timestamp_with_large_date(config, session_without_datasets):
    _ = config
    session = session_without_datasets
    query = "SELECT CAST('9999-12-31T05:00:00.123456' AS TIMESTAMP) AS TIMESTAMP"
    result = await session.execute_query(query)
    assert result.TIMESTAMP.tolist()[0] == pd.Timestamp("9999-12-31 05:00:00.123456")
