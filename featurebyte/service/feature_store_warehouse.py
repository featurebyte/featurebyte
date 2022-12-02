"""
Service for interacting with the data warehouse for queries around the feature store.

We split this into a separate service, as these typically require a session object that is created.
"""
from typing import Any, List

from featurebyte.models.feature_store import ColumnSpec, FeatureStoreModel
from featurebyte.persistent import Persistent
from featurebyte.routes.app_container import register_service_constructor
from featurebyte.service.base_service import BaseService
from featurebyte.service.session_manager import SessionManagerService


class FeatureStoreWarehouseService(BaseService):
    """
    FeatureStoreWarehouseService is responsible for interacting with the data warehouse.
    """

    def __init__(self, user: Any, persistent: Persistent):
        super().__init__(user, persistent)
        self.session_manager_service = SessionManagerService(user, persistent)

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
        return await db_session.list_tables(database_name=database_name, schema_name=schema_name)

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
        table_schema = await db_session.list_table_schema(
            database_name=database_name, schema_name=schema_name, table_name=table_name
        )
        return [ColumnSpec(name=name, dtype=dtype) for name, dtype in table_schema.items()]


register_service_constructor(FeatureStoreWarehouseService)
