"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Any, List

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
from featurebyte.service.session_validator import SessionValidatorService


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
        self.session_validator_service = SessionValidatorService(service.user, service.persistent)

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
        # Validate whether the feature store trying to be created, collides with another feature store
        #
        # OUTCOMES
        # exist in persistent | exist in DWH |           DWH                |     persistent
        #        Y            |      N       |          create              | error on write
        #        N            |      N       |          create              | write
        #        Y            |      Y       | if matches in DWH, no error  | error on write
        #                                    | if not, error                |
        #        N            |      Y       | if matches in DWH, no error  | write
        #                                    | if not, error                |
        _ = self.session_validator_service.validate_details(data.name, data.type, data.details)
        return await self.service.create_document(data)

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
        return await self.service.list_databases(
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
        return await self.service.list_schemas(
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
        return await self.service.list_tables(
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
        return await self.service.list_columns(
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
