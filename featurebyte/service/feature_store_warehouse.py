"""
Service for interacting with the data warehouse for queries around the feature store.

We split this into a separate service, as these typically require a session object that is created.
"""
from __future__ import annotations

from typing import Any, List, Optional

from featurebyte.exception import DatabaseNotFoundError, SchemaNotFoundError, TableNotFoundError
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService


class FeatureStoreWarehouseService:
    """
    FeatureStoreWarehouseService is responsible for interacting with the data warehouse.
    """

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
        self, feature_store: FeatureStoreModel, get_credential: Optional[Any]
    ) -> List[str]:
        """
        List databases in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        get_credential: Optional[Any]
            Get credential handler function

        Returns
        -------
        List[str]
            List of database names
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        return await db_session.list_databases()

    async def list_schemas(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
    ) -> List[str]:
        """
        List schemas in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use

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
            feature_store=feature_store
        )
        try:
            return await db_session.list_schemas(database_name=database_name)
        except db_session.no_schema_error as exc:
            raise DatabaseNotFoundError(f"Database {database_name} not found.") from exc

    async def list_tables(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
    ) -> List[str]:
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

        Raises
        ------
        SchemaNotFoundError
            If schema not found

        Returns
        -------
        List[str]
            List of table names
        """

        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
        )

        # check database exists
        await self.list_schemas(
            feature_store=feature_store,
            database_name=database_name,
        )

        try:
            tables = await db_session.list_tables(
                database_name=database_name, schema_name=schema_name
            )
        except db_session.no_schema_error as exc:
            raise SchemaNotFoundError(f"Schema {schema_name} not found.") from exc

        # exclude tables with names that has a "__" prefix
        return [table_name for table_name in tables if not table_name.startswith("__")]

    async def list_columns(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
        table_name: str,
    ) -> List[ColumnSpec]:
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

        Raises
        ------
        TableNotFoundError
            If table not found

        Returns
        -------
        List[ColumnSpec]
            List of ColumnSpec object
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
        )

        # check database and schema exists
        await self.list_tables(
            feature_store=feature_store,
            database_name=database_name,
            schema_name=schema_name,
        )

        try:
            table_schema = await db_session.list_table_schema(
                database_name=database_name, schema_name=schema_name, table_name=table_name
            )
        except db_session.no_schema_error as exc:
            raise TableNotFoundError(f"Table {table_name} not found.") from exc

        return [ColumnSpec(name=name, dtype=dtype) for name, dtype in table_schema.items()]
