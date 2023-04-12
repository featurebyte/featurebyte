"""
DatabricksSession class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Any, List, OrderedDict, cast

import collections

import pandas as pd
from pydantic import Field

from featurebyte import AccessTokenCredential
from featurebyte.enum import DBVarType, InternalName, SourceType
from featurebyte.logger import logger
from featurebyte.session.base import BaseSchemaInitializer, MetadataSchemaInitializer
from featurebyte.session.spark_aware import SparkAwareSchemaInitializer, SparkAwareSession

try:
    from databricks import sql as databricks_sql
    from databricks.sql.exc import ServerOperationError

    HAS_DATABRICKS_SQL_CONNECTOR = True
except ImportError:
    HAS_DATABRICKS_SQL_CONNECTOR = False


class DatabricksSession(SparkAwareSession):
    """
    Databricks session class
    """

    _no_schema_error = ServerOperationError

    # server_hostname: str
    source_type: SourceType = Field(SourceType.DATABRICKS, const=True)
    database_credential: AccessTokenCredential

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)

        if not HAS_DATABRICKS_SQL_CONNECTOR:
            raise RuntimeError("databricks-sql-connector is not available")

        self._initialize_storage()

        self._connection = databricks_sql.connect(
            server_hostname=data["host"],
            http_path=data["http_path"],
            access_token=self.database_credential.access_token,
            catalog=self.featurebyte_catalog,
            schema=self.featurebyte_schema,
        )

    def initializer(self) -> BaseSchemaInitializer:
        return DatabricksSchemaInitializer(self)

    @classmethod
    def is_threadsafe(cls) -> bool:
        return True

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
        if databricks_type not in mapping:
            logger.warning(f"Databricks: Not supported data type '{databricks_type}'")
        return mapping.get(databricks_type, DBVarType.UNKNOWN)

    def fetch_query_result_impl(self, cursor: Any) -> pd.DataFrame | None:
        arrow_table = cursor.fetchall_arrow()
        return arrow_table.to_pandas()


class DatabricksMetadataSchemaInitializer(MetadataSchemaInitializer):
    """Databricks metadata initializer class"""

    def create_metadata_table_queries(self, current_migration_version: int) -> List[str]:
        """Queries to create metadata table

        Parameters
        ----------
        current_migration_version: int
            Current migration version

        Returns
        -------
        List[str]
        """
        return [
            (
                "CREATE TABLE IF NOT EXISTS METADATA_SCHEMA ( "
                "WORKING_SCHEMA_VERSION INT, "
                f"{InternalName.MIGRATION_VERSION} INT, "
                "FEATURE_STORE_ID STRING, "
                "CREATED_AT TIMESTAMP "
                ");"
            ),
            (
                "INSERT INTO METADATA_SCHEMA "
                f"SELECT 0, {current_migration_version}, NULL, CURRENT_TIMESTAMP();"
            ),
        ]


class DatabricksSchemaInitializer(SparkAwareSchemaInitializer):
    """Databricks schema initializer class"""

    def __init__(self, session: DatabricksSession):
        super().__init__(session=session)
        self.session = cast(DatabricksSession, self.session)
        self.metadata_schema_initializer = DatabricksMetadataSchemaInitializer(session)

    @property
    def sql_directory_name(self) -> str:
        return "databricks"

    @property
    def current_working_schema_version(self) -> int:
        return 1
