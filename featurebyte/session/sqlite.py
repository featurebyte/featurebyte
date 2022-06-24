"""
SQLiteSession class
"""
from __future__ import annotations

from typing import Any

import os
import sqlite3

from pydantic import Field

from featurebyte.enum import DBVarType, SourceType
from featurebyte.session.base import BaseSession


class SQLiteSession(BaseSession):
    """
    SQLite session class
    """

    filename: str
    source_type: SourceType = Field(SourceType.SQLITE, const=True)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        filename = data["filename"]
        if not os.path.exists(filename):
            raise FileNotFoundError(f"SQLite file '{filename}' not found!")

        self._connection = sqlite3.connect(filename)

    def list_tables(self) -> list[str]:
        tables = self.execute_query("SELECT name FROM sqlite_master WHERE type = 'table'")
        output = []
        if tables is not None:
            output.extend(tables["name"])
        return output

    @staticmethod
    def _convert_to_internal_variable_type(sqlite_data_type: str) -> DBVarType:
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

    def list_table_schema(self, table_name: str) -> dict[str, DBVarType]:
        schema = self.execute_query(f'PRAGMA table_info("{table_name}")')
        column_name_type_map = {}
        if schema is not None:
            column_name_type_map = {
                column_name: self._convert_to_internal_variable_type(data_type)
                for _, (column_name, data_type) in schema[["name", "type"]].iterrows()
            }
        return column_name_type_map
