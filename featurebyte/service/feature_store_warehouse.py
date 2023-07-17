"""
Service for interacting with the data warehouse for queries around the feature store.

We split this into a separate service, as these typically require a session object that is created.
"""
from __future__ import annotations

from typing import Any, List

from featurebyte.exception import DatabaseNotFoundError, SchemaNotFoundError, TableNotFoundError
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession


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

    @classmethod
    async def check_database_exists(
        cls,
        db_session: BaseSession,
        database_name: str,
    ) -> None:
        """
        Check tables in feature store

        Parameters
        ----------
        db_session: BaseSession
            BaseSession object
        database_name: str
            Name of database to use

        Raises
        ------
        DatabaseNotFoundError
            If database does not exist
        """
        databases = await db_session.list_databases()
        databases = [database.lower() for database in databases]
        if database_name.lower() not in databases:
            raise DatabaseNotFoundError(f"Database {database_name} not found.")

    @classmethod
    async def check_schema_exists(
        cls,
        db_session: BaseSession,
        database_name: str,
        schema_name: str,
    ) -> None:
        """
        Check tables in feature store

        Parameters
        ----------
        db_session: BaseSession
            BaseSession object
        database_name: str
            Name of database to use
        schema_name: str
            Name of schema to use

        Raises
        ------
        SchemaNotFoundError
            If schema does not exist
        """
        await cls.check_database_exists(db_session=db_session, database_name=database_name)
        schemas = await db_session.list_schemas(database_name=database_name)
        schemas = [schema.lower() for schema in schemas]
        if schema_name.lower() not in schemas:
            raise SchemaNotFoundError(f"Schema {schema_name} not found.")

    @classmethod
    async def check_table_exists(
        cls,
        db_session: BaseSession,
        database_name: str,
        schema_name: str,
        table_name: str,
    ) -> None:
        """
        Check tables in feature store

        Parameters
        ----------
        db_session: BaseSession
            BaseSession object
        database_name: str
            Name of database to use
        schema_name: str
            Name of schema to use
        table_name: str
            Name of table to use

        Raises
        ------
        TableNotFoundError
            If table does not exist
        """
        await cls.check_schema_exists(
            db_session=db_session, database_name=database_name, schema_name=schema_name
        )
        tables = await db_session.list_tables(database_name=database_name, schema_name=schema_name)
        tables = [table.lower() for table in tables]
        if table_name.lower() not in tables:
            raise TableNotFoundError(f"Table {table_name} not found.")

    async def check_user_defined_function_exists(
        self,
        user_defined_function: UserDefinedFunctionModel,
        feature_store: FeatureStoreModel,
        get_credential: Any,
    ) -> None:
        """
        Check whether user defined function in feature store

        Parameters
        ----------
        user_defined_function: UserDefinedFunctionModel
            User defined function model
        feature_store: FeatureStoreModel
            Feature store model
        get_credential: Any
            Get credential handler function
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        await db_session.check_user_defined_function(user_defined_function=user_defined_function)

    async def list_databases(
        self, feature_store: FeatureStoreModel, get_credential: Any
    ) -> List[str]:
        """
        List databases in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        get_credential: Any
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
        get_credential: Any,
    ) -> List[str]:
        """
        List schemas in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use
        get_credential: Any
            Get credential handler function

        Returns
        -------
        List[str]
            List of schema names
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        # check database exists
        await self.check_database_exists(db_session=db_session, database_name=database_name)

        return await db_session.list_schemas(database_name=database_name)

    async def list_tables(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
        get_credential: Any,
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
        get_credential: Any
            Get credential handler function

        Returns
        -------
        List[str]
            List of table names
        """

        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )

        # check schema exists
        await self.check_schema_exists(
            db_session=db_session, database_name=database_name, schema_name=schema_name
        )

        tables = await db_session.list_tables(database_name=database_name, schema_name=schema_name)
        # exclude tables with names that has a "__" prefix
        return [table_name for table_name in tables if not table_name.startswith("__")]

    async def list_columns(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
        table_name: str,
        get_credential: Any,
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
        get_credential: Any
            Get credential handler function

        Returns
        -------
        List[ColumnSpec]
            List of ColumnSpec object
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )

        # check table exists
        await self.check_table_exists(
            db_session=db_session,
            database_name=database_name,
            schema_name=schema_name,
            table_name=table_name,
        )

        table_schema = await db_session.list_table_schema(
            database_name=database_name, schema_name=schema_name, table_name=table_name
        )
        return [ColumnSpec(name=name, dtype=dtype) for name, dtype in table_schema.items()]
