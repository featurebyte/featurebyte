"""
SQLiteSession class
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass

import pandas as pd

from featurebyte.enum import DBVarType
from featurebyte.session.base import AbstractSession, TableName, TableSchema
from featurebyte.session.enum import SourceType


@dataclass
class SQLiteSession(AbstractSession):
    """
    SQLite session class
    """

    filename: str
    source_type = SourceType.SQLITE

    def __post_init__(self) -> None:
        if not os.path.exists(self.filename):
            raise FileNotFoundError(f"SQLite file '{self.filename}' not found!")

        self._connection = sqlite3.connect(self.filename)
        super().__post_init__()

    def _list_tables(self) -> list[str]:
        query_table_res = self.execute_query("SELECT name FROM sqlite_master WHERE type = 'table'")
        return list(query_table_res["name"])

    @staticmethod
    def _get_db_var_type(sqlite_data_type: str) -> DBVarType:
        if "INT" in sqlite_data_type:
            return DBVarType.INT
        if "CHAR" in sqlite_data_type or "TEXT" in sqlite_data_type:
            return DBVarType.VARCHAR
        if (
            "REAL" in sqlite_data_type
            or "DOUBLE" in sqlite_data_type
            or "FLOAT" in sqlite_data_type
            or "DECIMAL" in sqlite_data_type
        ):
            return DBVarType.FLOAT
        if "BOOLEAN" in sqlite_data_type:
            return DBVarType.BOOL
        if "DATETIME" in sqlite_data_type:
            return DBVarType.TIMESTAMP
        if "DATE" in sqlite_data_type:
            return DBVarType.DATE
        raise ValueError(f"Not supported data type '{sqlite_data_type}'")

    def populate_database_metadata(self) -> dict[TableName, TableSchema]:
        output = {}
        for table in self._list_tables():
            query_column_res = self.execute_query(f"PRAGMA table_info('{table}')")
            column_name_type_map = {}
            for _, (column_name, data_type) in query_column_res[["name", "type"]].iterrows():
                column_name_type_map[column_name] = self._get_db_var_type(data_type)
            output[table] = column_name_type_map
        return output

    def execute_query(self, query: str) -> pd.DataFrame:
        cursor = self._connection.cursor()
        try:
            cursor.execute(query)
            all_rows = cursor.fetchall()
            columns = [row[0] for row in cursor.description]
            return pd.DataFrame(all_rows, columns=columns)
        finally:
            cursor.close()
