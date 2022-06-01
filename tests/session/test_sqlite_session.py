"""
Unit test for sqlite session
"""
import sqlite3
import tempfile

import pytest

from featurebyte.enum import DBVarType
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


def test_sqlite_session(sqlite_db_filename):
    """
    Test sqlite session
    """
    session = SQLiteSession(filename=sqlite_db_filename)
    assert session.database_metadata == {
        '"type_table"': {
            "int": DBVarType.INT,
            "integer": DBVarType.INT,
            "tinyint": DBVarType.INT,
            "smallint": DBVarType.INT,
            "mediumint": DBVarType.INT,
            "bigint": DBVarType.INT,
            "unsigned_big_int": DBVarType.INT,
            "int2": DBVarType.INT,
            "int8": DBVarType.INT,
            "char": DBVarType.VARCHAR,
            "varchar": DBVarType.VARCHAR,
            "varying_character": DBVarType.VARCHAR,
            "nchar": DBVarType.VARCHAR,
            "native_character": DBVarType.VARCHAR,
            "nvarchar": DBVarType.VARCHAR,
            "text": DBVarType.VARCHAR,
            "real": DBVarType.FLOAT,
            "double": DBVarType.FLOAT,
            "double_precision": DBVarType.FLOAT,
            "float": DBVarType.FLOAT,
            "decimal": DBVarType.FLOAT,
            "boolean": DBVarType.BOOL,
            "date": DBVarType.DATE,
            "datetime": DBVarType.TIMESTAMP,
        }
    }
