"""
FeatureStoreService class
"""
from __future__ import annotations

from typing import List, Type

from featurebyte.models.feature_store import ColumnSpec, FeatureStoreModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.feature_store import FeatureStoreCreate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.session_manager import SessionManagerService


class FeatureStoreService(
    BaseDocumentService[FeatureStoreModel, FeatureStoreCreate, BaseDocumentServiceUpdateSchema],
):
    """
    FeatureStoreService class
    """

    document_class: Type[FeatureStoreModel] = FeatureStoreModel

    @property
    def session_manager_service(self) -> SessionManagerService:
        """
        SessionManagerService object

        Returns
        -------
        SessionManagerService
        """
        return SessionManagerService(user=self.user, persistent=self.persistent)

    async def list_databases(self, feature_store: FeatureStoreModel) -> List[str]:
        """
        List databases in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object

        Returns
        -------
        List[str]
            List of database names
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
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

        Returns
        -------
        List[str]
            List of schema names
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
        )
        return await db_session.list_schemas(database_name=database_name)

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

        Returns
        -------
        List[str]
            List of table names
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
        )
        return await db_session.list_tables(database_name=database_name, schema_name=schema_name)

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

        Returns
        -------
        List[ColumnSpec]
            List of ColumnSpec object
        """
        db_session = await self.session_manager_service.get_feature_store_session(
            feature_store=feature_store
        )
        table_schema = await db_session.list_table_schema(
            database_name=database_name, schema_name=schema_name, table_name=table_name
        )
        return [ColumnSpec(name=name, dtype=dtype) for name, dtype in table_schema.items()]
