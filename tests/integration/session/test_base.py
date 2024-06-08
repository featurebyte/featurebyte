"""
Test base session
"""

from io import BytesIO

import pandas as pd
import pyarrow as pa
import pytest
import pytest_asyncio
from bson import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte.common.utils import ARROW_METADATA_DB_VAR_TYPE
from featurebyte.exception import DataWarehouseOperationError


def sample_dataframe():
    """
    Test dataframe
    """
    return pd.DataFrame(
        {
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
        }
    )


@pytest_asyncio.fixture(scope="module", autouse=True)
async def setup_module(session):
    """
    Setup module
    """
    await session.register_table("TEST_DATA_TABLE", sample_dataframe())


@pytest.mark.asyncio
async def test_execute_query(session):
    """
    Test execute query
    """
    query = "SELECT * FROM TEST_DATA_TABLE"
    df = await session.execute_query(query)
    expected_df = sample_dataframe()

    # expect arrays and dicts to be converted to strings
    assert_frame_equal(df, expected_df)


@pytest.mark.asyncio
async def test_fetch_empty(session):
    """
    Test fetch empty results
    """
    query = "SELECT * FROM TEST_DATA_TABLE WHERE 1 = 0"
    df = await session.execute_query(query)

    # expect empty dataframe with correct schema
    assert df.shape == (0, 10)
    assert df.dtypes.to_dict() == sample_dataframe().dtypes.to_dict()


@pytest.mark.asyncio
async def test_arrow_schema(session):
    """
    Test arrow schema and db variable type metadata
    """
    query = "SELECT * FROM TEST_DATA_TABLE"
    bytestream = session.get_async_query_stream(query)
    buffer = BytesIO()
    async for chunk in bytestream:
        buffer.write(chunk)
    buffer.seek(0)
    reader = pa.ipc.open_stream(buffer)
    arrow_table = reader.read_all()

    # expect arrays and dicts to be converted to strings
    expected_schema = pa.schema(
        [
            pa.field("bool", pa.bool_()),
            pa.field("int", pa.int64()),
            pa.field("float", pa.float64()),
            pa.field("string", pa.string()),
            pa.field("date", pa.timestamp("ns", tz=None)),
            pa.field("timestamp", pa.timestamp("ns", tz=None)),
            pa.field("int_list", pa.string()),
            pa.field("float_list", pa.string()),
            pa.field("string_list", pa.string()),
            pa.field("dict", pa.string()),
        ]
    )
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
        await session.execute_query(f"SELECT * FROM {name} LIMIT 1")


@pytest.mark.asyncio
async def test_drop_table__table_ok(session):
    """
    Test drop_table function
    """
    name = f"MY_TABLE_{str(ObjectId()).upper()}"
    await session.execute_query(f"CREATE TABLE {name} AS SELECT 1 AS A")
    await session.drop_table(
        table_name=name,
        schema_name=session.schema_name,
        database_name=session.database_name,
    )
    await check_table_does_not_exist(session, name)


@pytest.mark.asyncio
async def test_drop_table__table_error(session):
    """
    Test drop_table function
    """
    name = f"MY_TABLE_{str(ObjectId()).upper()}"
    await session.execute_query(f"CREATE TABLE {name} AS SELECT 1 AS A")
    with pytest.raises(DataWarehouseOperationError) as exc_info:
        await session.drop_table(
            table_name="wrong_name",
            schema_name=session.schema_name,
            database_name=session.database_name,
        )
    assert "wrong_name" in str(exc_info.value)


@pytest.mark.asyncio
async def test_drop_table__view_ok(session):
    """
    Test drop_table function
    """
    name = f"MY_VIEW_{str(ObjectId()).upper()}"
    await session.execute_query(f"CREATE VIEW {name} AS SELECT 1 AS A")
    await session.drop_table(
        table_name=name,
        schema_name=session.schema_name,
        database_name=session.database_name,
    )
    await check_table_does_not_exist(session, name)


@pytest.mark.asyncio
async def test_drop_table__view_error(session):
    """
    Test drop_table function
    """
    name = f"MY_VIEW_{str(ObjectId()).upper()}"
    await session.execute_query(f"CREATE VIEW {name} AS SELECT 1 AS A")
    with pytest.raises(DataWarehouseOperationError) as exc_info:
        await session.drop_table(
            table_name="wrong_name",
            schema_name=session.schema_name,
            database_name=session.database_name,
        )
    assert "wrong_name" in str(exc_info.value)


@pytest.mark.asyncio
async def test_execute_query__error_logging(session, caplog):
    """Test execute query error logging"""
    query = "SELECT * FROM TEST_DATA_TABLE"
    try:
        await session.execute_query(query)
    except Exception:  # pylint: disable=broad-except
        pass

    record = next(
        record for record in caplog.records if record.msg.startswith("Error executing query")
    )
    assert record.extra["query"] == query, record.extra
