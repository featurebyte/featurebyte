"""
FeatureStore API route controller
"""
from __future__ import annotations

from typing import Any, List

from bson.objectid import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models.credential import CredentialModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.credential import CredentialCreate
from featurebyte.schema.feature_store import (
    FeatureStoreCreate,
    FeatureStoreList,
    FeatureStorePreview,
    FeatureStoreSample,
    FeatureStoreShape,
)
from featurebyte.schema.info import FeatureStoreInfo
from featurebyte.service.credential import CredentialService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.preview import PreviewService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.session_validator import SessionValidatorService
from featurebyte.session.base import MetadataSchemaInitializer

logger = get_logger(__name__)


class FeatureStoreController(
    BaseDocumentController[FeatureStoreModel, FeatureStoreService, FeatureStoreList]
):
    """
    FeatureStore controller
    """

    paginated_document_class = FeatureStoreList

    def __init__(
        self,
        feature_store_service: FeatureStoreService,
        preview_service: PreviewService,
        session_manager_service: SessionManagerService,
        session_validator_service: SessionValidatorService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        credential_service: CredentialService,
    ):
        super().__init__(feature_store_service)
        self.preview_service = preview_service
        self.session_manager_service = session_manager_service
        self.session_validator_service = session_validator_service
        self.feature_store_warehouse_service = feature_store_warehouse_service
        self.credential_service = credential_service

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

        Raises
        ------
        Exception
            If feature store already exists or initialization fails
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
        #
        # Validate that feature store ID isn't claimed by the working schema.
        # If the feature store ID is already in use, this will throw an error.
        # Create the new feature store. If one already exists, we'll throw an error here.
        logger.debug("Start create_feature_store")
        document = await self.service.create_document(data)
        credential_doc = None
        try:
            # Check credentials
            credential = CredentialModel(
                name=document.name,
                feature_store_id=document.id,
                database_credential=data.database_credential,
                storage_credential=data.storage_credential,
            )

            async def _temp_get_credential(user_id: str, feature_store_name: str) -> Any:
                """
                Use the temporary credential to try to initialize the session.

                Parameters
                ----------
                user_id: str
                    user id
                feature_store_name: str
                    feature store name

                Returns
                -------
                Any
                    credentials
                """
                _ = user_id, feature_store_name
                return credential

            await self.session_validator_service.validate_feature_store_id_not_used_in_warehouse(
                feature_store_name=data.name,
                session_type=data.type,
                details=data.details,
                users_feature_store_id=document.id,
                get_credential=_temp_get_credential,
            )
            logger.debug("End validate_feature_store_id_not_used_in_warehouse")

            session = await self.session_manager_service.get_feature_store_session(
                feature_store=FeatureStoreModel(
                    name=data.name, type=data.type, details=data.details
                ),
                get_credential=_temp_get_credential,
            )
            # Try to persist credential
            credential_doc = await self.credential_service.create_document(
                data=CredentialCreate(**credential.dict(by_alias=True))
            )

            # If no error thrown from creating, try to create the metadata table with the feature store ID.
            metadata_schema_initializer = MetadataSchemaInitializer(session)
            await metadata_schema_initializer.update_feature_store_id(str(document.id))
        except Exception:
            # If there is an error, delete the feature store + credential and re-raise the error.
            await self.service.delete_document(document.id)
            if credential_doc:
                await self.credential_service.delete_document(credential_doc.id)
            raise

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

    async def shape(self, preview: FeatureStorePreview, get_credential: Any) -> FeatureStoreShape:
        """
        Retrieve shape for query graph node

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        FeatureStoreShape
            FeatureStoreShape object
        """
        return await self.preview_service.shape(preview=preview, get_credential=get_credential)

    async def preview(
        self, preview: FeatureStorePreview, limit: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Retrieve data preview for query graph node

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
        dict[str, Any]
            Dataframe converted to json string
        """
        return await self.preview_service.preview(
            preview=preview, limit=limit, get_credential=get_credential
        )

    async def sample(
        self, sample: FeatureStoreSample, size: int, seed: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Retrieve data sample for query graph node

        Parameters
        ----------
        sample: FeatureStoreSample
            FeatureStoreSample object
        size: int
            Maximum rows to sample
        seed: int
            Random seed to use for sampling
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        return await self.preview_service.sample(
            sample=sample, size=size, seed=seed, get_credential=get_credential
        )

    async def describe(
        self, sample: FeatureStoreSample, size: int, seed: int, get_credential: Any
    ) -> dict[str, Any]:
        """
        Retrieve data description for query graph node

        Parameters
        ----------
        sample: FeatureStoreSample
            FeatureStoreSample object
        size: int
            Maximum rows to sample
        seed: int
            Random seed to use for sampling
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        return await self.preview_service.describe(
            sample=sample, size=size, seed=seed, get_credential=get_credential
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
        info_document = await self.service.get_feature_store_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
