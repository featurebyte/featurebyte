"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import List

from bson.objectid import ObjectId

from featurebyte.models.feature_store import ColumnSpec, FeatureStoreModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature_store import (
    FeatureStoreCreate,
    FeatureStoreList,
    FeatureStorePreview,
)
from featurebyte.schema.info import FeatureStoreInfo
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.info import InfoService
from featurebyte.service.preview import PreviewService


class FeatureStoreController(
    BaseDocumentController[FeatureStoreModel, FeatureStoreService, FeatureStoreList]
):
    """
    FeatureStore controller
    """

    paginated_document_class = FeatureStoreList

    def __init__(
        self,
        service: FeatureStoreService,
        preview_service: PreviewService,
        info_service: InfoService,
    ):
        super().__init__(service)
        self.preview_service = preview_service
        self.info_service = info_service

    async def create_feature_store(
        self,
        data: FeatureStoreCreate,
    ) -> FeatureStoreModel:
        """
        Create Feature Store at persistent

        Parameters
        ----------
        data: FeatureStoreCreate
            FeatureStore creation payload

        Returns
        -------
        FeatureStoreModel
            Newly created feature store document
        """
        return await self.service.create_document(data)

    async def list_databases(
        self,
        feature_store: FeatureStoreModel,
    ) -> List[str]:
        """
        List databases accessible by the feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object

        Returns
        -------
        List[str]
            List of database names
        """
        return await self.service.list_databases(feature_store=feature_store)

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
        return await self.service.list_schemas(
            feature_store=feature_store,
            database_name=database_name,
        )

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
        return await self.service.list_tables(
            feature_store=feature_store,
            database_name=database_name,
            schema_name=schema_name,
        )

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
        return await self.service.list_columns(
            feature_store=feature_store,
            database_name=database_name,
            schema_name=schema_name,
            table_name=table_name,
        )

    async def preview(self, preview: FeatureStorePreview, limit: int) -> str:
        """
        Preview generic graph node

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        limit: int
            Row limit on preview results

        Returns
        -------
        str
            Dataframe converted to json string
        """
        return await self.preview_service.preview(preview=preview, limit=limit)

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> FeatureStoreInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        InfoDocument
        """
        info_document = await self.info_service.get_feature_store_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
