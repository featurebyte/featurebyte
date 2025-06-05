"""
SQLiteSession class
"""

from __future__ import annotations

import collections
import os
import sqlite3
from typing import Any, Optional, OrderedDict

import pandas as pd

from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.table import TableSpec
from featurebyte.session.base import (
    INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    BaseSchemaInitializer,
    BaseSession,
    QueryMetadata,
)


class SQLiteSession(BaseSession):
    """
    SQLite session class
    """

    filename: str
    source_type: SourceType = SourceType.SQLITE

    def initializer(self) -> Optional[BaseSchemaInitializer]:
        return None

    def _initialize_connection(self) -> None:
        filename = self.filename
        if not os.path.exists(filename):
            raise FileNotFoundError(f"SQLite file '{filename}' not found!")

        self._connection = sqlite3.connect(filename)

    @classmethod
    def is_threadsafe(cls) -> bool:
        return False

    async def _cancel_query(self, cursor: Any, query: str) -> bool:
        # no way to cancel query in sqlite
        return True

    async def _list_databases(self) -> list[str]:
        return []

    async def _list_schemas(self, database_name: str | None = None) -> list[str]:
        return []

    async def _list_tables(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> list[TableSpec]:
        tables = await self.execute_query(
            "SELECT name FROM sqlite_master WHERE type = 'table'", timeout=timeout
        )
        output = []
        if tables is not None:
            for _, (name,) in tables[["name"]].iterrows():
                output.append(TableSpec(name=name))
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
        timeout: float = INTERACTIVE_QUERY_TIMEOUT_SECONDS,
    ) -> OrderedDict[str, ColumnSpecWithDescription]:
        schema = await self.execute_query(f'PRAGMA table_info("{table_name}")', timeout=timeout)
        column_name_type_map = collections.OrderedDict()
        if schema is not None:
            for _, (column_name, data_type) in schema[["name", "type"]].iterrows():
                dtype = self._convert_to_internal_variable_type(data_type)
                column_name_type_map[column_name] = ColumnSpecWithDescription(
                    name=column_name,
                    dtype=dtype,
                    description=None,  # sqlite doesn't provide any meta for description
                )
        return column_name_type_map

    async def register_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        raise NotImplementedError()

    async def execute_query(
        self,
        query: str,
        timeout: float = 600,
        to_log_error: bool = True,
        query_result: QueryMetadata | None = None,
    ) -> pd.DataFrame | None:
        # sqlite session cannot be used in across threads
        _ = timeout
        return super().execute_query_blocking(query=query)

    async def comment_table(self, table_name: str, comment: str) -> None:
        pass

    async def comment_column(self, table_name: str, column_name: str, comment: str) -> None:
        pass
