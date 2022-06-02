"""
SnowflakeSession class
"""
from __future__ import annotations

from typing import Any

import json
import os
from dataclasses import dataclass, field

from snowflake import connector

from featurebyte.enum import DBVarType
from featurebyte.session.base import BaseSession, TableName, TableSchema
from featurebyte.session.enum import SnowflakeDataType, SourceType


@dataclass
class SnowflakeSession(BaseSession):
    """
    Snowflake session class
    """

    account: str
    warehouse: str
    database: str | None = field(default=None)
    schema: str | None = field(default=None)
    source_type = SourceType.SNOWFLAKE

    def __post_init__(self) -> None:
        if self.schema and self.database is None:
            raise ValueError("Database name is required if schema is set")

        user = os.getenv("SNOWFLAKE_USER")
        password = os.getenv("SNOWFLAKE_PASSWORD")

        if not user or not password:
            raise ValueError(
                "Environment variables 'SNOWFLAKE_USER' or 'SNOWFLAKE_PASSWORD' is not set"
            )

        self._connection = connector.connect(
            user=user,
            password=password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )
        super().__post_init__()

    def _list_databases(self) -> list[str]:
        if self.database:
            return [self.database]
        query_res = self.execute_query("SHOW DATABASES")
        if query_res is None:
            return []
        return list(query_res["name"])

    def _list_schemas(self) -> list[tuple[str, str]]:
        if self.database and self.schema:
            return [(self.database, self.schema)]
        database_schemas = []
        for database in self._list_databases():
            query_res = self.execute_query(f"SHOW SCHEMAS IN DATABASE {database}")
            if query_res is None:
                continue
            for schema in query_res["name"]:
                database_schemas.append((database, schema))
        return database_schemas

    def _list_tables_or_views(self) -> list[tuple[str, str, str]]:
        tables_or_views = []
        for database, schema in self._list_schemas():
            query_table_res = self.execute_query(f'SHOW TABLES IN SCHEMA "{database}"."{schema}"')
            if query_table_res is None:
                continue
            for table in query_table_res["name"]:
                tables_or_views.append((database, schema, table))

            query_view_res = self.execute_query(f'SHOW VIEWS IN SCHEMA "{database}"."{schema}"')
            if query_view_res is None:
                continue
            for view in query_view_res["name"]:
                tables_or_views.append((database, schema, view))
        return tables_or_views

    @staticmethod
    def _convert_to_db_var_type(snowflake_data_type: dict[str, Any]) -> DBVarType:
        data_type_to_db_var_type_map = {
            SnowflakeDataType.FIXED: DBVarType.INT,
            SnowflakeDataType.REAL: DBVarType.FLOAT,
            SnowflakeDataType.BINARY: DBVarType.BINARY,
            SnowflakeDataType.BOOLEAN: DBVarType.BOOL,
            SnowflakeDataType.DATE: DBVarType.DATE,
            SnowflakeDataType.TIME: DBVarType.TIME,
        }
        if snowflake_data_type["type"] in data_type_to_db_var_type_map:
            return data_type_to_db_var_type_map[snowflake_data_type["type"]]
        if snowflake_data_type["type"] == SnowflakeDataType.TEXT:
            return DBVarType.CHAR if snowflake_data_type["length"] == 1 else DBVarType.VARCHAR
        if snowflake_data_type["type"] in {
            SnowflakeDataType.TIMESTAMP_LTZ,
            SnowflakeDataType.TIMESTAMP_NTZ,
            SnowflakeDataType.TIMESTAMP_TZ,
        }:
            return DBVarType.TIMESTAMP
        raise ValueError(f"Not supported data type '{snowflake_data_type}'")

    def populate_database_metadata(self) -> dict[TableName, TableSchema]:
        output: dict[TableName, TableSchema] = {}
        for database, schema, table_or_view in self._list_tables_or_views():
            query_column_res = self.execute_query(
                f'SHOW COLUMNS IN "{database}"."{schema}"."{table_or_view}"'
            )
            if query_column_res is None:
                continue
            column_name_type_map = {}
            for _, (column_name, data_type) in query_column_res[
                ["column_name", "data_type"]
            ].iterrows():
                column_name_type_map[column_name] = self._convert_to_db_var_type(
                    json.loads(data_type)
                )
            output[f'"{database}"."{schema}"."{table_or_view}"'] = column_name_type_map
        return output
