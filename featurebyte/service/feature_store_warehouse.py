"""
Service for interacting with the data warehouse for queries around the feature store.

We split this into a separate service, as these typically require a session object that is created.
"""

from __future__ import annotations

import datetime
import functools
import os
from typing import Any, AsyncGenerator, Awaitable, Callable, List, Optional, Tuple, TypeVar, cast

from bson import ObjectId
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
from featurebyte.models.credential import CredentialModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.feature_store_cache import FeatureStoreCacheModel
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
from featurebyte.schema.feature_store_cache import FeatureStoreCacheCreate
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_cache import FeatureStoreCacheService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import (
    INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS,
    BaseSession,
)

MAX_TABLE_CELLS = int(
    os.environ.get("MAX_TABLE_CELLS", 10000000 * 300)
)  # 10 million rows, 300 columns

logger = get_logger(__name__)


T = TypeVar("T")


def async_cache(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
    @functools.wraps(func)
    async def wrapper(
        self: FeatureStoreWarehouseService,
        feature_store: FeatureStoreModel,
        *args: Any,
        refresh: bool = True,
        **kwargs: Any,
    ) -> T:
        credentials = await self.session_manager_service.credential_service.get_credentials(
            user_id=self.session_manager_service.user.id, feature_store=feature_store
        )
        key = str(hashkey(func.__name__, credentials.id if credentials else None, *args, **kwargs))
        query_filter = {"feature_store_id": feature_store.id, "key": key}

        if not refresh:
            # try to get from cache
            cached_results: List[
                FeatureStoreCacheModel
            ] = await self.feature_store_cache_service.list_documents(query_filter=query_filter)
            if cached_results:
                return cast(T, cached_results[0].value.value)

        result = await func(
            self, feature_store, *args, refresh=refresh, credentials=credentials, **kwargs
        )
        await self.feature_store_cache_service.delete_many(query_filter=query_filter)
        await self.feature_store_cache_service.create_document(
            data=FeatureStoreCacheCreate(
                feature_store_id=feature_store.id,
                key=key,
                value={"function_name": func.__name__, "value": result},
                **kwargs,
            )
        )
        return result

    return wrapper


class FeatureStoreWarehouseService:
    """
    FeatureStoreWarehouseService is responsible for interacting with the data warehouse.
    """

    session_initialization_timeout = INTERACTIVE_SESSION_TIMEOUT_SECONDS

    def __init__(
        self,
        session_manager_service: SessionManagerService,
        feature_store_service: FeatureStoreService,
        feature_store_cache_service: FeatureStoreCacheService,
    ):
        self.session_manager_service = session_manager_service
        self.feature_store_service = feature_store_service
        self.feature_store_cache_service = feature_store_cache_service

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

    @async_cache
    async def list_databases(
        self,
        feature_store: FeatureStoreModel,
        *,
        refresh: bool = True,
        credentials: Optional[CredentialModel] = None,
    ) -> List[str]:
        """
        List databases in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        refresh: bool
            Whether to refresh the list of databases
        credentials: Optional[CredentialModel] = None
            Credentials to use for the session. If None, will use the credentials from the session manager

        Returns
        -------
        List[str]
            List of database names
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
            credentials_override=credentials,
        )
        return await db_session.list_databases()

    @async_cache
    async def list_schemas(
        self,
        feature_store: FeatureStoreModel,
        *,
        refresh: bool = True,
        database_name: str,
        credentials: Optional[CredentialModel] = None,
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
        credentials: Optional[CredentialModel] = None
            Credentials to use for the session. If None, will use the credentials from the session manager

        Raises
        ------
        DatabaseNotFoundError
            If database not found

        Returns
        -------
        List[str]
            List of schema names
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
            credentials_override=credentials,
        )
        try:
            return await db_session.list_schemas(database_name=database_name)
        except db_session.no_schema_error as exc:
            raise DatabaseNotFoundError(f"Database {database_name} not found.") from exc

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
        if "TABLE" not in table_name:
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

    @async_cache
    async def list_tables(
        self,
        feature_store: FeatureStoreModel,
        *,
        refresh: bool = True,
        database_name: str,
        schema_name: str,
        credentials: Optional[CredentialModel] = None,
        session: Optional[BaseSession] = None,
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
        credentials: Optional[CredentialModel]
            Credentials to use for the session. If None, will use the credentials from the session manager
        session: Optional[BaseSession]
            Optional database session. If provided, will use this session instead of creating a new one.

        Raises
        ------
        SchemaNotFoundError
            If schema not found

        Returns
        -------
        List[TableSpec]
            List of tables
        """
        if session is not None:
            db_session = session
        else:
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

        return [
            table for table in tables if self._is_visible_table(table.name, is_featurebyte_schema)
        ]

    @async_cache
    async def list_columns(
        self,
        feature_store: FeatureStoreModel,
        *,
        refresh: bool = True,
        database_name: str,
        schema_name: str,
        table_name: str,
        credentials: Optional[CredentialModel] = None,
        session: Optional[BaseSession] = None,
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
        credentials: Optional[CredentialModel]
            Credentials to use for the session. If None, will use the credentials from the session manager
        session: Optional[BaseSession]
            Optional database session. If provided, will use this session instead of creating a new one.

        Raises
        ------
        TableNotFoundError
            If table not found

        Returns
        -------
        List[ColumnSpecWithDescription]
            List of ColumnSpecWithDescription object
        """
        if session is not None:
            db_session = session
        else:
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
            if col_name not in [InternalName.TABLE_ROW_INDEX, InternalName.TABLE_ROW_WEIGHT]
        }
        return list(table_schema.values())

    @async_cache
    async def get_table_details(
        self,
        feature_store: FeatureStoreModel,
        *,
        refresh: bool = True,
        database_name: str,
        schema_name: str,
        table_name: str,
        credentials: Optional[CredentialModel] = None,
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
        credentials: Optional[CredentialModel]
            Credentials to use for the session. If None, will use the credentials from the session manager

        Raises
        ------
        TableNotFoundError
            If table not found

        Returns
        -------
        TableDetails
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store,
            credentials_override=credentials,
        )
        try:
            return await db_session.get_table_details(
                database_name=database_name, schema_name=schema_name, table_name=table_name
            )
        except db_session.no_schema_error as exc:
            raise TableNotFoundError(f"Table {table_name} not found.") from exc

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
            if col_name not in [InternalName.TABLE_ROW_INDEX, InternalName.TABLE_ROW_WEIGHT]
        ]
        return (
            (result["row_count"].iloc[0], len(columns)),
            has_row_index,
            columns,
        )

    async def table_shape(
        self, location: TabularSource, session: Optional[BaseSession] = None
    ) -> FeatureStoreShape:
        """
        Get the shape table from location.

        Parameters
        ----------
        location: TabularSource
            Location to get shape from
        session: Optional[BaseSession]
            Optional session to use. If not provided, a new session will be created.

        Returns
        -------
        FeatureStoreShape
            Row and column counts
        """
        feature_store = await self.feature_store_service.get_document(
            document_id=location.feature_store_id
        )
        if session is not None:
            db_session = session
        else:
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

        columns_to_drop = []
        if result is not None:
            # drop row index column if present
            if InternalName.TABLE_ROW_INDEX in result.columns:
                columns_to_drop.append(InternalName.TABLE_ROW_INDEX)
            # drop sampling rate column if present
            if result is not None and InternalName.TABLE_ROW_WEIGHT in result.columns:
                columns_to_drop.append(InternalName.TABLE_ROW_WEIGHT)
            if columns_to_drop:
                result.drop(columns=columns_to_drop, inplace=True)

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

    async def clear_listing_cache(self, feature_store_id: ObjectId) -> None:
        """
        Clear the listing cache for databases, schemas, tables, and columns.

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store ID for which to clear the cache
        """
        await self.feature_store_cache_service.delete_many(
            query_filter={"feature_store_id": feature_store_id}
        )


class NonInteractiveFeatureStoreWarehouseService(FeatureStoreWarehouseService):
    """
    FeatureStoreWarehouseService for long-running queries
    """

    session_initialization_timeout = NON_INTERACTIVE_SESSION_TIMEOUT_SECONDS
