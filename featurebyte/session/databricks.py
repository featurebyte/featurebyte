"""
DatabricksSession class
"""
from __future__ import annotations

from typing import Any, OrderedDict

import collections
from pathlib import Path

import pandas as pd

try:
    from databricks import sql as databricks_sql

    HAS_DATABRICKS_SQL_CONNECTOR = True
except ImportError:
    HAS_DATABRICKS_SQL_CONNECTOR = False

from pydantic import Field

from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.session.base import BaseSession


class DatabricksSession(BaseSession):
    """
    Databricks session class
    """

    server_hostname: str
    http_path: str
    access_token: str
    featurebyte_catalog: str
    featurebyte_schema: str
    source_type: SourceType = Field(SourceType.DATABRICKS, const=True)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

        if not HAS_DATABRICKS_SQL_CONNECTOR:
            raise RuntimeError("databricks-sql-connector is not available")

        self._connection = databricks_sql.connect(
            server_hostname=data["server_hostname"],
            http_path=data["http_path"],
            access_token=data["access_token"],
            catalog=self.featurebyte_catalog,
            schema=self.featurebyte_schema,
        )

    async def list_databases(self) -> list[str]:
        cursor = self._connection.cursor().catalogs()
        df_result = super().fetch_query_result_impl(cursor)
        out = []
        if df_result is not None:
            out.extend(df_result["TABLE_CAT"])
        return out

    async def list_schemas(self, database_name: str | None = None) -> list[str]:
        cursor = self._connection.cursor().schemas(catalog_name=database_name)
        df_result = self.fetch_query_result_impl(cursor)
        out = []
        if df_result is not None:
            out.extend(df_result["TABLE_SCHEM"])
        return out

    async def list_tables(
        self, database_name: str | None = None, schema_name: str | None = None
    ) -> list[str]:
        cursor = self._connection.cursor().tables(
            catalog_name=database_name, schema_name=schema_name
        )
        df_result = super().fetch_query_result_impl(cursor)
        out = []
        if df_result is not None:
            out.extend(df_result["TABLE_NAME"])
        return out

    async def list_table_schema(
        self,
        table_name: str | None,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> OrderedDict[str, DBVarType]:
        cursor = self._connection.cursor().columns(
            table_name=table_name,
            catalog_name=database_name,
            schema_name=schema_name,
        )
        df_result = super().fetch_query_result_impl(cursor)
        column_name_type_map = collections.OrderedDict()
        if df_result is not None:
            for _, (column_name, databricks_type_name) in df_result[
                ["COLUMN_NAME", "TYPE_NAME"]
            ].iterrows():
                column_name_type_map[column_name] = self._convert_to_internal_variable_type(
                    databricks_type_name
                )
        return column_name_type_map

    @staticmethod
    def _convert_to_internal_variable_type(databricks_type: str) -> DBVarType:
        if databricks_type.endswith("INT"):
            # BIGINT, INT, SMALLINT, TINYINT
            return DBVarType.INT
        mapping = {
            "BINARY": DBVarType.BINARY,
            "BOOLEAN": DBVarType.BOOL,
            "DATE": DBVarType.DATE,
            "DECIMAL": DBVarType.FLOAT,
            "DOUBLE": DBVarType.FLOAT,
            "FLOAT": DBVarType.FLOAT,
            "INTERVAL": DBVarType.TIMEDELTA,
            "VOID": DBVarType.VOID,
            "TIMESTAMP": DBVarType.TIMESTAMP,
            "ARRAY": DBVarType.ARRAY,
            "MAP": DBVarType.MAP,
            "STRUCT": DBVarType.STRUCT,
            "STRING": DBVarType.VARCHAR,
        }
        return mapping[databricks_type]

    def fetch_query_result_impl(self, cursor: Any) -> pd.DataFrame | None:
        arrow_table = cursor.fetchall_arrow()
        return arrow_table.to_pandas()

    async def execute_async_query(
        self, query: str, timeout: int = 180, output_path: Path | None = None
    ) -> pd.DataFrame | None:
        _ = timeout
        result = await self.execute_query(query)
        if output_path is None:
            return result
        assert isinstance(result, pd.DataFrame)
        result.to_parquet(output_path)
        return None

    async def register_temp_table(self, table_name: str, dataframe: pd.DataFrame) -> None:
        date_cols = dataframe.select_dtypes(include=["datetime64"]).columns.tolist()
        table_expr = construct_dataframe_sql_expr(dataframe, date_cols).sql(
            pretty=True, dialect="spark"
        )
        query = f"CREATE OR REPLACE TEMPORARY VIEW {table_name} AS {table_expr}"
        await self.execute_query(query)
