"""
Service for interacting with the data warehouse for queries around the feature store.

We split this into a separate service, as these typically require a session object that is created.
"""

from __future__ import annotations

import datetime
import os
from typing import Any, AsyncGenerator, List, Optional, Tuple

from cachetools import TTLCache
from cachetools.keys import hashkey

from featurebyte.common.utils import dataframe_to_json
from featurebyte.enum import DBVarType, InternalName, MaterializedTableNamePrefix, ViewNamePrefix
from featurebyte.exception import (
    DatabaseNotFoundError,
    LimitExceededError,
    SchemaNotFoundError,
    TableNotFoundError,
)
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.table import TableDetails, TableSpec
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.materialisation import (
    get_feature_store_id_expr,
    get_source_count_expr,
    get_source_expr,
)
from featurebyte.schema.feature_store import (
    FeatureStoreQueryPreview,
    FeatureStoreShape,
    validate_select_sql,
)
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import (
    INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    BaseSession,
)

MAX_TABLE_CELLS = int(
    os.environ.get("MAX_TABLE_CELLS", 10000000 * 300)
)  # 10 million rows, 300 columns

DATABASE_CACHE_MAX_SIZE = 100
DATABASE_CACHE_TTL_SECONDS = 3600

list_database_cache: TTLCache[Any, list[str]] = TTLCache(
    maxsize=DATABASE_CACHE_MAX_SIZE, ttl=DATABASE_CACHE_TTL_SECONDS
)
list_schema_cache: TTLCache[Any, list[str]] = TTLCache(
    maxsize=DATABASE_CACHE_MAX_SIZE, ttl=DATABASE_CACHE_TTL_SECONDS
)
list_table_cache: TTLCache[Any, list[TableSpec]] = TTLCache(
    maxsize=DATABASE_CACHE_MAX_SIZE, ttl=DATABASE_CACHE_TTL_SECONDS
)
list_column_cache: TTLCache[Any, list[ColumnSpecWithDescription]] = TTLCache(
    maxsize=DATABASE_CACHE_MAX_SIZE, ttl=DATABASE_CACHE_TTL_SECONDS
)
table_details_cache: TTLCache[Any, TableDetails] = TTLCache(
    maxsize=DATABASE_CACHE_MAX_SIZE, ttl=DATABASE_CACHE_TTL_SECONDS
)

logger = get_logger(__name__)


class FeatureStoreWarehouseService:
    """
    FeatureStoreWarehouseService is responsible for interacting with the data warehouse.
    """

    session_initialization_timeout = INTERACTIVE_SESSION_TIMEOUT_SECONDS

    def __init__(
        self,
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
    ):
        self.session_manager_service = session_manager_service
        self.feature_store_service = feature_store_service

    async def check_user_defined_function_exists(
        self,
        user_defined_function: UserDefinedFunctionModel,
        feature_store: FeatureStoreModel,
    ) -> None:
        """
        Check whether user defined function in feature store

        Parameters
        ----------
        user_defined_function: UserDefinedFunctionModel
            User defined function model
        feature_store: FeatureStoreModel
            Feature store model
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
        )
        await db_session.check_user_defined_function(user_defined_function=user_defined_function)

    async def list_databases(
        self,
        feature_store: FeatureStoreModel,
        refresh: bool = True,
    ) -> List[str]:
        """
        List databases in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        refresh: bool
            Whether to refresh the list of databases

        Returns
        -------
        List[str]
            List of database names
        """
        if refresh:
            list_database_cache.clear()

        credentials = await self.session_manager_service.credential_service.get_credentials(
            user_id=self.session_manager_service.user.id, feature_store=feature_store
        )
        key = hashkey(credentials.id if credentials else None, feature_store.id)
        result = list_database_cache.get(key)
        if result is None:
            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=feature_store,
                credentials_override=credentials,
            )
            result = await db_session.list_databases()
            list_database_cache[key] = result
        return result

    async def list_schemas(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        refresh: bool = True,
    ) -> List[str]:
        """
        List schemas in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use
        refresh: bool
            Whether to refresh the list of schemas

        Raises
        ------
        DatabaseNotFoundError
            If database not found

        Returns
        -------
        List[str]
            List of schema names
        """
        if refresh:
            list_schema_cache.clear()
        credentials = await self.session_manager_service.credential_service.get_credentials(
            user_id=self.session_manager_service.user.id, feature_store=feature_store
        )
        key = hashkey(credentials.id if credentials else None, feature_store.id, database_name)
        result = list_schema_cache.get(key)
        if result is None:
            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=feature_store,
                credentials_override=credentials,
            )
            try:
                result = await db_session.list_schemas(database_name=database_name)
                list_schema_cache[key] = result
            except db_session.no_schema_error as exc:
                raise DatabaseNotFoundError(f"Database {database_name} not found.") from exc
        return result

    @staticmethod
    def _is_visible_table(table_name: str, filter_featurebyte_tables: bool) -> bool:
        table_name = table_name.upper()
        if table_name.startswith("__"):
            return False
        if not filter_featurebyte_tables:
            return True
        for prefix in ViewNamePrefix.visible():
            # Table name case can get changed in certain databases (e.g. Databricks)
            if table_name.startswith(prefix.upper()):
                return True
        # quick filter for materialized tables
        if "TABLE" not in table_name.upper():
            return False
        for prefix in MaterializedTableNamePrefix.visible():
            # Table name case can get changed in certain databases (e.g. Databricks)
            if table_name.startswith(prefix.upper()):
                return True
        return False

    @staticmethod
    async def _is_featurebyte_schema(
        db_session: BaseSession,
        database_name: str,
        schema_name: str,
        tables: List[TableSpec],
    ) -> bool:
        try:
            # check if metadata_schema table exists
            for table in tables:
                if table.name.upper() == "METADATA_SCHEMA":
                    sql_expr = get_feature_store_id_expr(
                        database_name=database_name, schema_name=schema_name
                    )
                    sql = sql_to_string(
                        sql_expr,
                        source_type=db_session.source_type,
                    )
                    _ = await db_session.execute_query(sql)
                    return True
            return False
        except db_session.no_schema_error:
            return False

    async def list_tables(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
        refresh: bool = True,
    ) -> List[TableSpec]:
        """
        List tables in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use
        schema_name: str
            Name of schema to use
        refresh: bool
            Whether to refresh the list of tables

        Raises
        ------
        SchemaNotFoundError
            If schema not found

        Returns
        -------
        List[TableSpec]
            List of tables
        """
        if refresh:
            list_table_cache.clear()
        credentials = await self.session_manager_service.credential_service.get_credentials(
            user_id=self.session_manager_service.user.id, feature_store=feature_store
        )
        key = hashkey(
            credentials.id if credentials else None, feature_store.id, database_name, schema_name
        )
        result = list_table_cache.get(key)
        logger.info(f"Cached {result is None}")
        if result is None:
            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=feature_store,
                credentials_override=credentials,
            )
            try:
                tables = await db_session.list_tables(
                    database_name=database_name, schema_name=schema_name
                )
            except db_session.no_schema_error as exc:
                raise SchemaNotFoundError(f"Schema {schema_name} not found.") from exc
            is_featurebyte_schema = await self._is_featurebyte_schema(
                db_session,
                database_name,
                schema_name,
                tables,
            )

            result = [
                table
                for table in tables
                if self._is_visible_table(table.name, is_featurebyte_schema)
            ]
            list_table_cache[key] = result
        return result

    async def list_columns(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
        table_name: str,
        refresh: bool = True,
    ) -> List[ColumnSpecWithDescription]:
        """
        List columns in database table

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use
        schema_name: str
            Name of schema to use
        table_name: str
            Name of table to use
        refresh: bool
            Whether to refresh the list of columns

        Raises
        ------
        TableNotFoundError
            If table not found

        Returns
        -------
        List[ColumnSpecWithDescription]
            List of ColumnSpecWithDescription object
        """
        if refresh:
            list_column_cache.clear()
        credentials = await self.session_manager_service.credential_service.get_credentials(
            user_id=self.session_manager_service.user.id, feature_store=feature_store
        )
        key = hashkey(
            credentials.id if credentials else None,
            feature_store.id,
            database_name,
            schema_name,
            table_name,
        )
        result = list_column_cache.get(key)
        if result is None:
            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=feature_store,
                credentials_override=credentials,
            )

            try:
                table_schema = await db_session.list_table_schema(
                    database_name=database_name, schema_name=schema_name, table_name=table_name
                )
            except db_session.no_schema_error as exc:
                raise TableNotFoundError(f"Table {table_name} not found.") from exc

            table_schema = {  # type: ignore[assignment]
                col_name: v
                for (col_name, v) in table_schema.items()
                if col_name != InternalName.TABLE_ROW_INDEX
            }
            result = list(table_schema.values())
            list_column_cache[key] = result
        return result

    async def get_table_details(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
        table_name: str,
        refresh: bool = True,
    ) -> TableDetails:
        """
        Get table details

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use
        schema_name: str
            Name of schema to use
        table_name: str
            Name of table to use
        refresh: bool
            Whether to refresh the table details

        Raises
        ------
        TableNotFoundError
            If table not found

        Returns
        -------
        TableDetails
        """
        if refresh:
            table_details_cache.clear()
        credentials = await self.session_manager_service.credential_service.get_credentials(
            user_id=self.session_manager_service.user.id, feature_store=feature_store
        )
        key = hashkey(
            credentials.id if credentials else None,
            feature_store.id,
            database_name,
            schema_name,
            table_name,
        )
        result = table_details_cache.get(key)
        if result is None:
            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=feature_store,
                credentials_override=credentials,
            )
            try:
                result = await db_session.get_table_details(
                    database_name=database_name, schema_name=schema_name, table_name=table_name
                )
                table_details_cache[key] = result
            except db_session.no_schema_error as exc:
                raise TableNotFoundError(f"Table {table_name} not found.") from exc
        return result

    async def _get_table_shape(
        self, location: TabularSource, db_session: BaseSession
    ) -> Tuple[Tuple[int, int], bool, list[str]]:
        # check size of the table
        sql_expr = get_source_count_expr(source=location.table_details)
        sql = sql_to_string(
            sql_expr,
            source_type=db_session.source_type,
        )
        result = await db_session.execute_query(sql)
        assert result is not None
        columns_specs = await db_session.list_table_schema(**location.table_details.json_dict())
        has_row_index = InternalName.TABLE_ROW_INDEX in columns_specs
        columns = [
            col_name
            for col_name in columns_specs.keys()
            if col_name != InternalName.TABLE_ROW_INDEX
        ]
        return (
            (result["row_count"].iloc[0], len(columns)),
            has_row_index,
            columns,
        )

    async def table_shape(self, location: TabularSource) -> FeatureStoreShape:
        """
        Get the shape table from location.

        Parameters
        ----------
        location: TabularSource
            Location to get shape from

        Returns
        -------
        FeatureStoreShape
            Row and column counts
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=location.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, timeout=self.session_initialization_timeout
        )
        shape, _, _ = await self._get_table_shape(location, db_session)
        return FeatureStoreShape(num_rows=shape[0], num_cols=shape[1])

    async def table_preview(
        self,
        location: TabularSource,
        limit: int,
        order_by_column: Optional[str] = None,
        column_names: Optional[List[str]] = None,
    ) -> dict[str, Any]:
        """
        Preview table from location.

        Parameters
        ----------
        location: TabularSource
            Location to preview from
        limit: int
            Row limit on preview results
        order_by_column: Optional[str]
            Column to order by
        column_names: Optional[List[str]]
            Columns to project

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=location.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, timeout=self.session_initialization_timeout
        )
        sql_expr = get_source_expr(source=location.table_details, column_names=column_names)

        # apply order by if specified
        if order_by_column:
            sql_expr = sql_expr.order_by(quoted_identifier(order_by_column))

        # apply limit
        sql_expr = sql_expr.limit(limit)
        sql = sql_to_string(
            sql_expr,
            source_type=db_session.source_type,
        )
        result = await db_session.execute_query(sql)

        # drop row index column if present
        if result is not None and InternalName.TABLE_ROW_INDEX in result.columns:
            result.drop(columns=[InternalName.TABLE_ROW_INDEX], inplace=True)

        # convert to json
        type_conversions = {}
        if result is not None:
            for col_name in result.columns:
                if result.shape[0] > 0:
                    if isinstance(result[col_name].iloc[0], datetime.datetime):
                        type_conversions[col_name] = DBVarType.TIMESTAMP
                    elif isinstance(result[col_name].iloc[0], datetime.date):
                        type_conversions[col_name] = DBVarType.DATE
        return dataframe_to_json(result, type_conversions=type_conversions or None)

    async def sql_preview(
        self,
        preview: FeatureStoreQueryPreview,
        limit: int,
    ) -> dict[str, Any]:
        """
        Preview table from location.

        Parameters
        ----------
        preview: FeatureStoreQueryPreview
            Preview object containing SQL query and feature store ID
        limit: int
            Row limit on preview results

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=preview.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, timeout=self.session_initialization_timeout
        )
        # validate that the SQL is a single select statement
        sql_expr = validate_select_sql(preview.sql, db_session.source_type)

        # apply limit
        sql_expr = sql_expr.limit(limit)
        sql = sql_to_string(
            sql_expr,
            source_type=db_session.source_type,
        )
        result = await db_session.execute_query(sql)
        return dataframe_to_json(result)

    async def download_table(
        self,
        location: TabularSource,
    ) -> Optional[AsyncGenerator[bytes, None]]:
        """
        Download table from location.

        Parameters
        ----------
        location: TabularSource
            Location to download from

        Returns
        -------
        AsyncGenerator[bytes, None]
            Asynchronous bytes generator

        Raises
        ------
        LimitExceededError
            Table size exceeds the limit.
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=location.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, timeout=self.session_initialization_timeout
        )

        shape, has_row_index, columns = await self._get_table_shape(location, db_session)
        logger.debug(
            "Downloading table from feature store",
            extra={
                "location": location.json_dict(),
                "shape": shape,
            },
        )

        if shape[0] * shape[1] > MAX_TABLE_CELLS:
            raise LimitExceededError(f"Table size {shape} exceeds download limit.")

        sql_expr = get_source_expr(source=location.table_details, column_names=columns)
        if has_row_index:
            sql_expr = sql_expr.order_by(quoted_identifier(InternalName.TABLE_ROW_INDEX))
        sql = sql_to_string(
            sql_expr,
            source_type=db_session.source_type,
        )
        return db_session.get_async_query_stream(sql)


class NonInteractiveFeatureStoreWarehouseService(FeatureStoreWarehouseService):
    """
    FeatureStoreWarehouseService for long-running queries
    """

    session_initialization_timeout = NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS
