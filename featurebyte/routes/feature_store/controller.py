"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Any, List

from bson.objectid import ObjectId

from featurebyte.models.feature_store import ColumnSpec, FeatureStoreModel
from featurebyte.routes.app_container import register_controller_constructor
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.feature_store import (
    FeatureStoreCreate,
    FeatureStoreList,
    FeatureStorePreview,
)
from featurebyte.schema.info import FeatureStoreInfo
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.info import InfoService
from featurebyte.service.preview import PreviewService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.session_validator import SessionValidatorService
from featurebyte.session.base import MetadataSchemaInitializer


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
        session_manager_service: SessionManagerService,
        session_validator_service: SessionValidatorService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
    ):
        super().__init__(service)
        self.preview_service = preview_service
        self.info_service = info_service
        self.session_manager_service = session_manager_service
        self.session_validator_service = session_validator_service
        self.feature_store_warehouse_service = feature_store_warehouse_service

    async def create_feature_store(
        self,
        data: FeatureStoreCreate,
        get_credential: Any,
        skip_validation: bool = True,
    ) -> FeatureStoreModel:
        """
        Create Feature Store at persistent

        Parameters
        ----------
        data: FeatureStoreCreate
            FeatureStore creation payload
        get_credential: Any
            credential handler function
        skip_validation: bool
            decide whether to skip validation

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

        # If skip validation, just create the document. This is the current default behaviour until we resolve
        # some issues with how the SaaS version has a deadlock between creating a session, and writing credentials
        # to the collection.
        if skip_validation:
            return await self.service.create_document(data)

        # Validate that feature store ID isn't claimed by the working schema.
        # If the feature store ID is already in use, this will throw an error.
        async with self.service.persistent.start_transaction():
            # Create the new feature store. If one already exists, we'll throw an error here.
            document = await self.service.create_document(data)
            await self.session_validator_service.validate_feature_store_id_not_used_in_warehouse(
                feature_store_name=data.name,
                session_type=data.type,
                details=data.details,
                get_credential=get_credential,
                users_feature_store_id=document.id,
            )
            # Retrieve a session for initializing
            session = await self.session_manager_service.get_feature_store_session(
                feature_store=FeatureStoreModel(
                    name=data.name, type=data.type, details=data.details
                ),
                get_credential=get_credential,
            )
            # If no error thrown from creating, try to create the metadata table with the feature store ID.
            metadata_schema_initializer = MetadataSchemaInitializer(session)
            await metadata_schema_initializer.update_feature_store_id(str(document.id))

        return document

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
        return await self.feature_store_warehouse_service.list_databases(
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
        return await self.feature_store_warehouse_service.list_schemas(
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
        return await self.feature_store_warehouse_service.list_tables(
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
        return await self.feature_store_warehouse_service.list_columns(
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


register_controller_constructor(
    FeatureStoreController,
    [
        FeatureStoreService,
        PreviewService,
        InfoService,
        SessionManagerService,
        SessionValidatorService,
        FeatureStoreWarehouseService,
    ],
)
