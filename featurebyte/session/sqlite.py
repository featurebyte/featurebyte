"""
SQLiteSession class
"""
from __future__ import annotations

from typing import Any, Optional, OrderedDict

import collections
import os
import sqlite3

import pandas as pd
from pydantic import Field

from featurebyte.enum import DBVarType, SourceType
from featurebyte.session.base import BaseSchemaInitializer, BaseSession


class SQLiteSession(BaseSession):
    """
    SQLite session class
    """

    filename: str
    source_type: SourceType = Field(SourceType.SQLITE, const=True)

    def initializer(self) -> Optional[BaseSchemaInitializer]:
        return None

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        filename = data["filename"]
        if not os.path.exists(filename):
            raise FileNotFoundError(f"SQLite file '{filename}' not found!")

        self._connection = sqlite3.connect(filename)

    @property
    def database_name(self) -> str:
        raise NotImplementedError()

    @property
    def schema_name(self) -> str:
        raise NotImplementedError()

    @classmethod
    def is_threadsafe(cls) -> bool:
        return False

    async def list_databases(self) -> list[str]:
        return []

    async def list_schemas(self, database_name: str | None = None) -> list[str]:
        return []

    async def list_tables(
        self, database_name: str | None = None, schema_name: str | None = None
    ) -> list[str]:
        tables = await self.execute_query("SELECT name FROM sqlite_master WHERE type = 'table'")
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
        if "DATETIME" in sqlite_data_type or "TIMESTAMP" in sqlite_data_type:
            return DBVarType.TIMESTAMP
        if "DATE" in sqlite_data_type:
            return DBVarType.DATE
        raise ValueError(f"Not supported data type '{sqlite_data_type}'")

    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> OrderedDict[str, DBVarType]:
        schema = await self.execute_query(f'PRAGMA table_info("{table_name}")')
        column_name_type_map = collections.OrderedDict()
        if schema is not None:
            for _, (column_name, data_type) in schema[["name", "type"]].iterrows():
                column_name_type_map[column_name] = self._convert_to_internal_variable_type(
                    data_type
                )
        return column_name_type_map

    async def register_table(
        self, table_name: str, dataframe: pd.DataFrame, temporary: bool = True
    ) -> None:
        raise NotImplementedError()

    async def execute_query(self, query: str, timeout: float = 600) -> pd.DataFrame | None:
        # sqlite session cannot be used in across threads
        _ = timeout
        return super().execute_query_blocking(query=query)
