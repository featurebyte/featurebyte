"""
SQLiteSession class
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass

from featurebyte.enum import DBVarType, SourceType
from featurebyte.session.base import BaseSession, TableSchema


@dataclass
class SQLiteSession(BaseSession):
    """
    SQLite session class
    """

    filename: str
    source_type = SourceType.SQLITE

    def __post_init__(self) -> None:
        if not os.path.exists(self.filename):
            raise FileNotFoundError(f"SQLite file '{self.filename}' not found!")

        self.connection = sqlite3.connect(self.filename)
        super().__post_init__()

    def _list_tables(self) -> list[str]:
        query_table_res = self.execute_query("SELECT name FROM sqlite_master WHERE type = 'table'")
        if query_table_res is None:
            return []
        return list(query_table_res["name"])

    @staticmethod
    def _convert_to_db_var_type(sqlite_data_type: str) -> DBVarType:
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

    def populate_database_metadata(self) -> dict[str, TableSchema]:
        output: dict[str, TableSchema] = {}
        for table in self._list_tables():
            query_column_res = self.execute_query(f'PRAGMA table_info("{table}")')
            if query_column_res is None:
                continue
            column_name_type_map = {}
            for _, (column_name, data_type) in query_column_res[["name", "type"]].iterrows():
                column_name_type_map[column_name] = self._convert_to_db_var_type(data_type)
            output[f'"{table}"'] = column_name_type_map
        return output
