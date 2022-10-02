"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Any, List, cast

from featurebyte.models.feature_store import ColumnSpec, FeatureStoreModel
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.feature_store import (
    FeatureStoreCreate,
    FeatureStoreInfo,
    FeatureStoreList,
    FeatureStorePreview,
)
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.preview import PreviewService


class FeatureStoreController(  # type: ignore[misc]
    BaseDocumentController[FeatureStoreModel, FeatureStoreList],
    GetInfoControllerMixin[FeatureStoreInfo],
):
    """
    FeatureStore controller
    """

    paginated_document_class = FeatureStoreList

    def __init__(self, service: FeatureStoreService, preview_service: PreviewService):
        super().__init__(service)  # type: ignore[arg-type]
        self.preview_service = preview_service

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
        document = await self.service.create_document(data)
        return cast(FeatureStoreModel, document)

    async def list_databases(
        self,
        feature_store: FeatureStoreModel,
        get_credential: Any,
    ) -> List[str]:
        """
        List databases accessible by the feature store

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
        service = cast(FeatureStoreService, self.service)
        return await service.list_databases(
            feature_store=feature_store, get_credential=get_credential
        )

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
        service = cast(FeatureStoreService, self.service)
        return await service.list_schemas(
            feature_store=feature_store,
            database_name=database_name,
            get_credential=get_credential,
        )

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
        service = cast(FeatureStoreService, self.service)
        return await service.list_tables(
            feature_store=feature_store,
            database_name=database_name,
            schema_name=schema_name,
            get_credential=get_credential,
        )

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
        service = cast(FeatureStoreService, self.service)
        return await service.list_columns(
            feature_store=feature_store,
            database_name=database_name,
            schema_name=schema_name,
            table_name=table_name,
            get_credential=get_credential,
        )

    async def preview(self, preview: FeatureStorePreview, limit: int, get_credential: Any) -> str:
        """
        Preview generic graph node

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        limit: int
            Row limit on preview results
        get_credential: Any
            Get credential handler function

        Returns
        -------
        str
            Dataframe converted to json string
        """
        return await self.preview_service.preview(
            preview=preview, limit=limit, get_credential=get_credential
        )
