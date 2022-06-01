"""
SnowflakeSession class
"""
from __future__ import annotations

from typing import Any, Optional

import json
import os
from dataclasses import dataclass, field

import pandas as pd
from snowflake import connector

from featurebyte.enum import DBVarType
from featurebyte.session.base import AbstractSession, TableName, TableSchema
from featurebyte.session.enum import SnowflakeDataType, SourceType


@dataclass
class SnowflakeSession(AbstractSession):
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

        self._connection = connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
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
        return list(query_res["name"])

    def _list_schemas(self) -> list[tuple[str, str]]:
        if self.database and self.schema:
            return [(self.database, self.schema)]
        database_schemas = []
        for database in self._list_databases():
            query_res = self.execute_query("SHOW SCHEMAS IN DATABASE %s" % database)
            for schema in query_res["name"]:
                database_schemas.append((database, schema))
        return database_schemas

    def _list_tables_or_views(self) -> list[tuple[str, str, str]]:
        table_or_views = []
        for full_schema in self._list_schemas():
            query_table_res = self.execute_query('SHOW TABLES IN SCHEMA "%s"."%s"' % full_schema)
            for table_name in query_table_res["name"]:
                table_or_views.append(full_schema + (table_name,))

            query_view_res = self.execute_query('SHOW VIEWS IN SCHEMA "%s"."%s"' % full_schema)
            for view_name in query_view_res["name"]:
                table_or_views.append(full_schema + (view_name,))
        return table_or_views

    @staticmethod
    def _get_db_var_type(snowflake_data_type: dict[str, Any]) -> DBVarType:
        if snowflake_data_type["type"] == SnowflakeDataType.FIXED:
            return DBVarType.INT
        if snowflake_data_type["type"] == SnowflakeDataType.REAL:
            return DBVarType.FLOAT
        if snowflake_data_type["type"] == SnowflakeDataType.TEXT:
            return DBVarType.CHAR if snowflake_data_type["length"] == 1 else DBVarType.VARCHAR
        if snowflake_data_type["type"] == SnowflakeDataType.BINARY:
            return DBVarType.BINARY
        if snowflake_data_type["type"] == SnowflakeDataType.BOOLEAN:
            return DBVarType.BOOL
        if snowflake_data_type["type"] == SnowflakeDataType.DATE:
            return DBVarType.DATE
        if snowflake_data_type["type"] == SnowflakeDataType.TIME:
            return DBVarType.TIME
        if snowflake_data_type["type"] in {
            SnowflakeDataType.TIMESTAMP_LTZ,
            SnowflakeDataType.TIMESTAMP_NTZ,
            SnowflakeDataType.TIMESTAMP_TZ,
        }:
            return DBVarType.TIMESTAMP
        raise ValueError(f"Not supported data type '{snowflake_data_type}'")

    def populate_database_metadata(self) -> dict[TableName, TableSchema]:
        output = {}
        for full_table_path in self._list_tables_or_views():
            query_column_res = self.execute_query(
                'SHOW COLUMNS IN TABLE "%s"."%s"."%s"' % full_table_path
            )
            column_name_type_map = {}
            for _, (column_name, data_type) in query_column_res[
                ["column_name", "data_type"]
            ].iterrows():
                column_name_type_map[column_name] = self._get_db_var_type(json.loads(data_type))
            table_name = '"%s"."%s"."%s"' % full_table_path
            output[table_name] = column_name_type_map
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
