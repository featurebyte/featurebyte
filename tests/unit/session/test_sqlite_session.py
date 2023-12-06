"""
Unit test for sqlite session
"""
import sqlite3
import tempfile

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.session.sqlite import SQLiteSession


@pytest.fixture(name="sqlite_db_filename")
def sqlite_db_file():
    """
    Create SQLite database file for testing
    """
    with tempfile.NamedTemporaryFile() as file_handle:
        connection = sqlite3.connect(file_handle.name)
        cursor = connection.cursor()
        query = """
        CREATE TABLE type_table(
            int INT,
            integer INTEGER,
            tinyint TINYINT,
            smallint SMALLINT,
            mediumint MEDIUMINT,
            bigint BIGINT,
            unsigned_big_int UNSIGNED BIG INT,
            int2 INT2,
            int8 INT8,
            char CHARACTER(20),
            varchar VARCHAR(255),
            varying_character VARYING CHARACTER(255),
            nchar NCHAR(55),
            native_character NATIVE CHARACTER(70),
            nvarchar NVARCHAR(100),
            text TEXT,
            real REAL,
            double DOUBLE,
            double_precision DOUBLE PRECISION,
            float FLOAT,
            decimal DECIMAL(10,5),
            boolean BOOLEAN,
            date DATE,
            datetime DATETIME
        )
        """
        cursor.execute(query)
        connection.commit()
        yield file_handle.name


def test_sqlite_session__file_not_found():
    """
    Test sqlite session when the specified sqlite file not found
    """
    with pytest.raises(FileNotFoundError) as exc:
        SQLiteSession(filename="some_random_sqlite_file.db")
    assert "SQLite file 'some_random_sqlite_file.db' not found!" in str(exc.value)


@pytest.mark.asyncio
async def test_sqlite_session(sqlite_db_filename):
    """
    Test sqlite session
    """
    session = SQLiteSession(filename=sqlite_db_filename)
    assert not await session.list_databases()
    assert not await session.list_schemas()
    assert await session.list_tables() == ["type_table"]
    assert await session.list_table_schema(table_name="type_table") == {
        "int": ColumnSpecWithDescription(name="int", dtype=DBVarType.INT),
        "integer": ColumnSpecWithDescription(name="integer", dtype=DBVarType.INT),
        "tinyint": ColumnSpecWithDescription(name="tinyint", dtype=DBVarType.INT),
        "smallint": ColumnSpecWithDescription(name="smallint", dtype=DBVarType.INT),
        "mediumint": ColumnSpecWithDescription(name="mediumint", dtype=DBVarType.INT),
        "bigint": ColumnSpecWithDescription(name="bigint", dtype=DBVarType.INT),
        "unsigned_big_int": ColumnSpecWithDescription(name="unsigned_big_int", dtype=DBVarType.INT),
        "int2": ColumnSpecWithDescription(name="int2", dtype=DBVarType.INT),
        "int8": ColumnSpecWithDescription(name="int8", dtype=DBVarType.INT),
        "char": ColumnSpecWithDescription(name="char", dtype=DBVarType.VARCHAR),
        "varchar": ColumnSpecWithDescription(name="varchar", dtype=DBVarType.VARCHAR),
        "varying_character": ColumnSpecWithDescription(
            name="varying_character", dtype=DBVarType.VARCHAR
        ),
        "nchar": ColumnSpecWithDescription(name="nchar", dtype=DBVarType.VARCHAR),
        "native_character": ColumnSpecWithDescription(
            name="native_character", dtype=DBVarType.VARCHAR
        ),
        "nvarchar": ColumnSpecWithDescription(name="nvarchar", dtype=DBVarType.VARCHAR),
        "text": ColumnSpecWithDescription(name="text", dtype=DBVarType.VARCHAR),
        "real": ColumnSpecWithDescription(name="real", dtype=DBVarType.FLOAT),
        "double": ColumnSpecWithDescription(name="double", dtype=DBVarType.FLOAT),
        "double_precision": ColumnSpecWithDescription(
            name="double_precision", dtype=DBVarType.FLOAT
        ),
        "float": ColumnSpecWithDescription(name="float", dtype=DBVarType.FLOAT),
        "decimal": ColumnSpecWithDescription(name="decimal", dtype=DBVarType.FLOAT),
        "boolean": ColumnSpecWithDescription(name="boolean", dtype=DBVarType.BOOL),
        "date": ColumnSpecWithDescription(name="date", dtype=DBVarType.DATE),
        "datetime": ColumnSpecWithDescription(name="datetime", dtype=DBVarType.TIMESTAMP),
    }


@pytest.mark.asyncio
async def test_execute_query__with_empty_return(sqlite_db_filename):
    """
    Test execute query with empty result
    """
    session = SQLiteSession(filename=sqlite_db_filename)
    write_output = await session.execute_query(
        """
        CREATE TEMPORARY TABLE temp_table(int INT)
        """
    )
    assert write_output is None
