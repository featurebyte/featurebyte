"""
FeatureStore API route controller
"""

from __future__ import annotations

from typing import Any, List, Tuple

from bson import ObjectId

from featurebyte.exception import DataWarehouseConnectionError, FeatureStoreSchemaCollisionError
from featurebyte.logging import get_logger
from featurebyte.models.credential import CredentialModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.credential import CredentialCreate
from featurebyte.schema.feature_store import (
    DatabaseDetailsServiceUpdate,
    DatabaseDetailsUpdate,
    FeatureStoreCreate,
    FeatureStoreList,
    FeatureStorePreview,
    FeatureStoreQueryPreview,
    FeatureStoreSample,
    FeatureStoreShape,
    FeatureStoreUpdate,
)
from featurebyte.schema.info import FeatureStoreInfo
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.data_description import DataDescriptionTaskPayload
from featurebyte.service.catalog import AllCatalogService
from featurebyte.service.credential import CredentialService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.preview import PreviewService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.session.base import BaseSession, MetadataSchemaInitializer

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
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        credential_service: CredentialService,
        all_catalog_service: AllCatalogService,
        task_controller: TaskController,
    ):
        super().__init__(feature_store_service)
        self.preview_service = preview_service
        self.session_manager_service = session_manager_service
        self.feature_store_warehouse_service = feature_store_warehouse_service
        self.credential_service = credential_service
        self.all_catalog_service = all_catalog_service
        self.task_controller = task_controller

    @staticmethod
    async def _check_feature_store_ownership(
        db_session: BaseSession, feature_store_id: ObjectId
    ) -> bool:
        """
        Check whether the DWH is already bound to a feature store.

        Parameters
        ----------
        db_session: BaseSession
            Database session object
        feature_store_id: ObjectId
            Feature store ID that you propose it to be bound to the DWH

        Returns
        -------
        bool
            True if the DWH is not bound to any feature store or the feature store ID matches the registered feature store ID
            False if the DWH is already bound to another feature store
        """
        working_schema_metadata = await db_session.get_working_schema_metadata()
        registered_feature_store_id = working_schema_metadata.get("feature_store_id")

        # FeatureStore is not in use
        # We can bind the feature store (mongodb.feature_store.id <=> DWH.metadata.feature_store_id)
        if not registered_feature_store_id:
            return True

        # FeatureStore is in use
        # But the feature store id matches the registered feature store id
        if feature_store_id == registered_feature_store_id:
            return True
        return False

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
        FeatureStoreSchemaCollisionError
            If feature store already exists or initialization fails
        Exception
            If any other error occurs
        """
        # Validate whether the feature store trying to be created, collides with another feature store
        # This occurs when you tried to bootstrap a new feature store with the same config as an existing one.
        # 1. Person A creates a feature store with config Q.
        #      + UUID Z is created in the mongodb.feature_store.id
        #      + UUID Z is also stored in the DWH.(schema).metadata.feature_store_id
        # 2. Person B tries to create a new feature store with the same config Q.
        #      + UUID X is created in the mongodb.feature_store.id
        #      + UUID Z is ALREADY stored in the DWH.(schema).metadata.feature_store_id
        #      + A conflict occurs because the DWH is already bound to a previous feature store.
        #      + We reject the creation of the new feature store and rollback the creation of the mongodb.feature_store.

        document = await self.service.create_document(data)
        credential_doc = None
        try:
            # Check credentials
            credentials = CredentialModel(
                name=document.name,
                feature_store_id=document.id,
                database_credential=data.database_credential,
                storage_credential=data.storage_credential,
            )
            db_session = await self.session_manager_service.get_feature_store_session(
                feature_store=FeatureStoreModel(
                    name=data.name, type=data.type, details=data.details
                ),
                credentials_override=credentials,
            )

            if not await self._check_feature_store_ownership(db_session, document.id):
                raise FeatureStoreSchemaCollisionError(
                    "DWH.feature_store is already owned by another mongodb.feature_store.id"
                )

            # Try to persist credential
            credential_doc = await self.credential_service.create_document(
                data=CredentialCreate(**credentials.model_dump(by_alias=True))
            )

            # If no error thrown from creating, try to create the metadata table with the feature store ID.
            metadata_schema_initializer = MetadataSchemaInitializer(db_session)
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
        return await self.feature_store_warehouse_service.list_databases(
            feature_store=feature_store,
        )

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
        return await self.feature_store_warehouse_service.list_schemas(
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
        tables = await self.feature_store_warehouse_service.list_tables(
            feature_store=feature_store,
            database_name=database_name,
            schema_name=schema_name,
        )
        return [table.name for table in tables]

    async def list_columns(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
        table_name: str,
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

        Returns
        -------
        List[ColumnInfo]
            List of ColumnInfo object
        """
        return await self.feature_store_warehouse_service.list_columns(
            feature_store=feature_store,
            database_name=database_name,
            schema_name=schema_name,
            table_name=table_name,
        )

    async def shape(self, preview: FeatureStorePreview) -> FeatureStoreShape:
        """
        Retrieve shape for query graph node

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object

        Returns
        -------
        FeatureStoreShape
            FeatureStoreShape object
        """
        return await self.preview_service.shape(preview=preview, allow_long_running=False)

    async def table_shape(self, location: TabularSource) -> FeatureStoreShape:
        """
        Retrieve shape for tabular source

        Parameters
        ----------
        location: TabularSource
            TabularSource object

        Returns
        -------
        FeatureStoreShape
            FeatureStoreShape object
        """
        return await self.feature_store_warehouse_service.table_shape(location=location)

    async def preview(self, preview: FeatureStorePreview, limit: int) -> dict[str, Any]:
        """
        Retrieve data preview for query graph node

        Parameters
        ----------
        preview: FeatureStorePreview
            FeatureStorePreview object
        limit: int
            Row limit on preview results

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        return await self.preview_service.preview(
            preview=preview, limit=limit, allow_long_running=False
        )

    async def sql_preview(self, preview: FeatureStoreQueryPreview, limit: int) -> dict[str, Any]:
        """
        Retrieve data preview for sql statement

        Parameters
        ----------
        preview: FeatureStoreQueryPreview
            FeatureStoreQueryPreview object
        limit: int
            Row limit on preview results

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        return await self.feature_store_warehouse_service.sql_preview(preview=preview, limit=limit)

    async def table_preview(self, location: TabularSource, limit: int) -> dict[str, Any]:
        """
        Retrieve data preview for tabular source

        Parameters
        ----------
        location: TabularSource
            TabularSource object
        limit: int
            Row limit on preview results

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        return await self.feature_store_warehouse_service.table_preview(
            location=location, limit=limit
        )

    async def sample(self, sample: FeatureStoreSample, size: int, seed: int) -> dict[str, Any]:
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

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        return await self.preview_service.sample(
            sample=sample, size=size, seed=seed, allow_long_running=False
        )

    async def describe(self, sample: FeatureStoreSample, size: int, seed: int) -> dict[str, Any]:
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

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string
        """
        return await self.preview_service.describe(
            sample=sample, size=size, seed=seed, allow_long_running=False
        )

    async def create_data_description(
        self, sample: FeatureStoreSample, size: int, seed: int, catalog_id: ObjectId
    ) -> Task:
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
        catalog_id: ObjectId
            Catalog ID used for task submission

        Returns
        -------
        Task
        """

        # prepare task payload and submit task
        payload = DataDescriptionTaskPayload(
            sample=sample,
            size=size,
            seed=seed,
            user_id=self.task_controller.task_manager.user.id,
            catalog_id=catalog_id,
        )
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [(self.all_catalog_service, {"default_feature_store_ids": document_id})]

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

    async def update_details(
        self, feature_store_id: ObjectId, data: DatabaseDetailsUpdate
    ) -> FeatureStoreModel:
        """
        Update feature store details

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store ID
        data: DatabaseDetailsUpdate
            DatabaseDetailsUpdate object

        Returns
        -------
        FeatureStoreModel
            Updated feature store document

        Raises
        ------
        DataWarehouseConnectionError
            If invalid details
        """
        document: FeatureStoreModel = await self.service.get_document(feature_store_id)
        # ensure fields are updatable
        details_dict = data.model_dump(exclude_none=True)
        for key, _ in details_dict.items():
            assert key in document.details.updatable_fields, f"Field is not updatable: {key}"
        updated_details = {**document.details.model_dump(by_alias=True), **details_dict}

        # test connection works with new details
        update_data = DatabaseDetailsServiceUpdate(details=updated_details)
        document.details = update_data.details
        session = await self.session_manager_service.get_feature_store_session(
            feature_store=document,
        )
        try:
            await session.list_databases()
        except session.no_schema_error as exc:
            raise DataWarehouseConnectionError(f"Invalid details: {exc}") from exc

        # Update valid details
        await self.service.update_document(document_id=feature_store_id, data=update_data)
        return await self.service.get_document(feature_store_id)

    async def update(
        self, feature_store_id: ObjectId, data: FeatureStoreUpdate
    ) -> FeatureStoreModel:
        """
        Update feature store

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store ID
        data: FeatureStoreUpdate
            FeatureStoreUpdate object

        Returns
        -------
        FeatureStoreModel
            Updated feature store document
        """
        await self.service.update_document(document_id=feature_store_id, data=data)
        return await self.service.get_document(feature_store_id)
