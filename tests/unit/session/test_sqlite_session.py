"""
Unit test for sqlite session
"""

import sqlite3
import tempfile

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDetails
from featurebyte.query_graph.model.table import TableDetails
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
    tables = await session.list_tables()
    assert [table.name for table in tables] == ["type_table"]
    assert await session.list_table_schema(table_name="type_table") == {
        "int": ColumnSpecWithDetails(name="int", dtype=DBVarType.INT),
        "integer": ColumnSpecWithDetails(name="integer", dtype=DBVarType.INT),
        "tinyint": ColumnSpecWithDetails(name="tinyint", dtype=DBVarType.INT),
        "smallint": ColumnSpecWithDetails(name="smallint", dtype=DBVarType.INT),
        "mediumint": ColumnSpecWithDetails(name="mediumint", dtype=DBVarType.INT),
        "bigint": ColumnSpecWithDetails(name="bigint", dtype=DBVarType.INT),
        "unsigned_big_int": ColumnSpecWithDetails(name="unsigned_big_int", dtype=DBVarType.INT),
        "int2": ColumnSpecWithDetails(name="int2", dtype=DBVarType.INT),
        "int8": ColumnSpecWithDetails(name="int8", dtype=DBVarType.INT),
        "char": ColumnSpecWithDetails(name="char", dtype=DBVarType.VARCHAR),
        "varchar": ColumnSpecWithDetails(name="varchar", dtype=DBVarType.VARCHAR),
        "varying_character": ColumnSpecWithDetails(
            name="varying_character", dtype=DBVarType.VARCHAR
        ),
        "nchar": ColumnSpecWithDetails(name="nchar", dtype=DBVarType.VARCHAR),
        "native_character": ColumnSpecWithDetails(name="native_character", dtype=DBVarType.VARCHAR),
        "nvarchar": ColumnSpecWithDetails(name="nvarchar", dtype=DBVarType.VARCHAR),
        "text": ColumnSpecWithDetails(name="text", dtype=DBVarType.VARCHAR),
        "real": ColumnSpecWithDetails(name="real", dtype=DBVarType.FLOAT),
        "double": ColumnSpecWithDetails(name="double", dtype=DBVarType.FLOAT),
        "double_precision": ColumnSpecWithDetails(name="double_precision", dtype=DBVarType.FLOAT),
        "float": ColumnSpecWithDetails(name="float", dtype=DBVarType.FLOAT),
        "decimal": ColumnSpecWithDetails(name="decimal", dtype=DBVarType.FLOAT),
        "boolean": ColumnSpecWithDetails(name="boolean", dtype=DBVarType.BOOL),
        "date": ColumnSpecWithDetails(name="date", dtype=DBVarType.DATE),
        "datetime": ColumnSpecWithDetails(name="datetime", dtype=DBVarType.TIMESTAMP),
    }
    assert await session.get_table_details(table_name="type_table") == TableDetails(
        fully_qualified_name="type_table"
    )


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
