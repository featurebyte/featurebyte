"""
DevelopmentDataset API route controller
"""

from __future__ import annotations

from typing import cast

from bson import ObjectId

from featurebyte.exception import DocumentDeletionError
from featurebyte.logging import get_logger
from featurebyte.models.development_dataset import DevelopmentDatasetModel, DevelopmentTable
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.development_dataset import (
    DevelopmentDatasetCreate,
    DevelopmentDatasetInfo,
    DevelopmentDatasetList,
    DevelopmentDatasetServiceUpdate,
    DevelopmentDatasetUpdate,
    DevelopmentTableInfo,
)
from featurebyte.schema.task import Task
from featurebyte.service.catalog import CatalogService
from featurebyte.service.development_dataset import DevelopmentDatasetService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.table import TableService
from featurebyte.service.task_manager import TaskManager

logger = get_logger(__name__)


class DevelopmentDatasetController(
    BaseDocumentController[
        DevelopmentDatasetModel, DevelopmentDatasetService, DevelopmentDatasetList
    ]
):
    """
    DevelopmentDataset controller
    """

    paginated_document_class = DevelopmentDatasetList
    document_update_schema_class = DevelopmentDatasetUpdate

    def __init__(
        self,
        development_dataset_service: DevelopmentDatasetService,
        table_service: TableService,
        feature_store_service: FeatureStoreService,
        catalog_service: CatalogService,
        task_controller: TaskController,
        task_manager: TaskManager,
    ):
        super().__init__(development_dataset_service)
        self.table_service = table_service
        self.feature_store_service = feature_store_service
        self.catalog_service = catalog_service
        self.task_controller = task_controller
        self.task_manager = task_manager

        # retrieve active catalog id and feature store id
        assert development_dataset_service.catalog_id is not None
        self.active_catalog_id: ObjectId = development_dataset_service.catalog_id

    async def _get_feature_store_id(self) -> ObjectId:
        """
        Get feature store id from active catalog

        Returns
        -------
        ObjectId
        """
        active_catalog = await self.catalog_service.get_document(document_id=self.active_catalog_id)
        return ObjectId(active_catalog.default_feature_store_ids[0])

    async def create_development_dataset(
        self,
        data: DevelopmentDatasetCreate,
    ) -> Task:
        """
        Create Online Store at persistent

        Parameters
        ----------
        data: DevelopmentDatasetCreate
            DevelopmentDataset creation payload

        Returns
        -------
        Task
        """
        payload = await self.service.get_development_dataset_create_task_payload(data=data)
        task_id = await self.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def update_development_dataset(
        self, development_dataset_id: ObjectId, data: DevelopmentDatasetUpdate
    ) -> DevelopmentDatasetModel:
        """
        Update online store

        Parameters
        ----------
        development_dataset_id: ObjectId
            DevelopmentDataset ID
        data: DevelopmentDatasetUpdate
            DevelopmentDataset update payload

        Returns
        -------
        DevelopmentDatasetModel
            Updated online store document
        """
        update = DevelopmentDatasetServiceUpdate(**data.model_dump())
        return cast(
            DevelopmentDatasetModel,
            await self.service.update_document(development_dataset_id, update),
        )

    async def delete_development_dataset(self, document_id: ObjectId) -> Task:
        """
        Delete DevelopmentDataset

        Parameters
        ----------
        document_id: ObjectId
            Document ID

        Returns
        -------
        Task
        """
        # check if document exists
        _ = await self.service.get_document(document_id=document_id)

        # check if document is used by any other documents
        await self.verify_operation_by_checking_reference(
            document_id=document_id, exception_class=DocumentDeletionError
        )

        # create task payload & submit task
        payload = await self.service.get_development_dataset_delete_task_payload(
            document_id=document_id
        )
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def add_development_tables(
        self, development_dataset_id: ObjectId, development_tables: list[DevelopmentTable]
    ) -> Task:
        """
        Add development tables to a development dataset

        Parameters
        ----------
        development_dataset_id: ObjectId
            Document ID of the development dataset to which tables will be added
        development_tables: list[DevelopmentTable]
            List of DevelopmentTableInfo to be added

        Returns
        -------
        Task
        """
        payload = await self.service.get_development_dataset_add_tables_task_payload(
            document_id=development_dataset_id, development_tables=development_tables
        )
        task_id = await self.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def get_info(self, document_id: ObjectId, verbose: bool) -> DevelopmentDatasetInfo:
        """
        Get DevelopmentDataset info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose flag

        Returns
        -------
        DevelopmentDatasetInfo
        """
        _ = verbose

        document = await self.service.get_document(document_id=document_id)

        # Retrieve feature store names
        feature_store_id_to_name = {}
        async for doc in self.feature_store_service.list_documents_as_dict_iterator(
            query_filter={
                "_id": {
                    "$in": list(
                        set([
                            dev_table.location.feature_store_id
                            for dev_table in document.development_tables
                        ])
                    )
                }
            },
            projection={"_id": 1, "name": 1},
        ):
            feature_store_id_to_name[doc["_id"]] = doc["name"]

        table_id_to_name = {}
        async for doc in self.table_service.list_documents_as_dict_iterator(
            query_filter={
                "_id": {"$in": [dev_table.table_id for dev_table in document.development_tables]}
            },
            projection={"_id": 1, "name": 1},
        ):
            table_id_to_name[doc["_id"]] = doc["name"]

        return DevelopmentDatasetInfo(
            name=document.name,
            created_at=document.created_at,
            updated_at=document.updated_at,
            description=document.description,
            sample_from_timestamp=document.sample_from_timestamp,
            sample_to_timestamp=document.sample_to_timestamp,
            development_tables=[
                DevelopmentTableInfo(
                    table_name=table_id_to_name[dev_table.table_id],
                    feature_store_name=feature_store_id_to_name[
                        dev_table.location.feature_store_id
                    ],
                    table_details=dev_table.location.table_details,
                )
                for dev_table in document.development_tables
            ],
        )
