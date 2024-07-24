"""
Catalog API route controller
"""

from __future__ import annotations

from typing import Optional

from bson import ObjectId

from featurebyte.exception import DocumentDeletionError
from featurebyte.models.catalog import CatalogModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.catalog import (
    CatalogList,
    CatalogOnlineStoreUpdate,
    CatalogServiceUpdate,
    CatalogUpdate,
)
from featurebyte.schema.info import CatalogInfo
from featurebyte.schema.task import Task
from featurebyte.service.catalog import CatalogService
from featurebyte.service.deployment import AllDeploymentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_store import OnlineStoreService


class CatalogController(
    BaseDocumentController[CatalogModel, CatalogService, CatalogList],
):
    """
    Catalog Controller
    """

    paginated_document_class = CatalogList

    def __init__(
        self,
        service: CatalogService,
        all_deployment_service: AllDeploymentService,
        feature_store_service: FeatureStoreService,
        online_store_service: OnlineStoreService,
        task_controller: TaskController,
    ):
        super().__init__(service=service)
        self.all_deployment_service = all_deployment_service
        self.feature_store_service = feature_store_service
        self.online_store_service = online_store_service
        self.task_controller = task_controller

    async def update_catalog(
        self,
        catalog_id: ObjectId,
        data: CatalogUpdate,
    ) -> CatalogModel:
        """
        Update Catalog stored at persistent

        Parameters
        ----------
        catalog_id: ObjectId
            Catalog ID
        data: CatalogUpdate
            Catalog update payload

        Returns
        -------
        CatalogModel
            Catalog object with updated attribute(s)
        """
        await self.service.update_document(
            document_id=catalog_id,
            data=CatalogServiceUpdate(**data.model_dump()),
            return_document=False,
        )
        return await self.get(document_id=catalog_id)

    async def update_catalog_online_store(
        self,
        catalog_id: ObjectId,
        data: CatalogOnlineStoreUpdate,
    ) -> CatalogModel:
        """
        Update Catalog online store stored at persistent

        Parameters
        ----------
        catalog_id: ObjectId
            Catalog ID
        data: CatalogOnlineStoreUpdate
            Catalog online store update payload

        Returns
        -------
        CatalogModel
            Catalog object with updated attribute(s)
        """
        await self.update_catalog_online_store_async(catalog_id, data)
        await self.service.update_online_store(
            document_id=catalog_id,
            data=data,
        )
        return await self.get(document_id=catalog_id)

    async def update_catalog_online_store_async(
        self,
        catalog_id: ObjectId,
        data: CatalogOnlineStoreUpdate,
    ) -> Optional[Task]:
        """
        Update Catalog online store by triggering a CATALOG_ONLINE_STORE_UPDATE task

        Parameters
        ----------
        catalog_id: ObjectId
            Catalog ID
        data: CatalogOnlineStoreUpdate
            Catalog online store update payload

        Returns
        -------
        Optional[Task]
        """
        payload = await self.service.get_update_online_store_task_payload(
            document_id=catalog_id,
            data=data,
        )
        if payload is None:
            return None
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=task_id)

    async def delete_catalog(self, catalog_id: ObjectId, soft_delete: bool) -> None:
        """
        Delete a document given document ID

        Parameters
        ----------
        catalog_id: ObjectId
            Catalog ID
        soft_delete: bool
            Flag to control soft delete

        Raises
        ------
        NotImplementedError
            If hard delete is requested
        DocumentDeletionError
            If the catalog has active deployment
        """
        if not soft_delete:
            raise NotImplementedError("Hard delete is not supported")

        async for doc in self.all_deployment_service.list_documents_as_dict_iterator(
            query_filter={"enabled": True, "catalog_id": catalog_id}
        ):
            raise DocumentDeletionError(
                f"Catalog cannot be deleted because it still has active deployment: {doc['name']}"
            )

        await self.service.soft_delete_document(document_id=catalog_id)

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> CatalogInfo:
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
        _ = verbose
        catalog = await self.service.get_document(document_id=document_id)

        # retrieve feature store name
        if catalog.default_feature_store_ids:
            feature_store = await self.feature_store_service.get_document(
                document_id=catalog.default_feature_store_ids[0]
            )
            feature_store_name = feature_store.name
        else:
            feature_store_name = None

        # retrieve online store name
        if catalog.online_store_id:
            online_store = await self.online_store_service.get_document(
                document_id=catalog.online_store_id
            )
            online_store_name = online_store.name
        else:
            online_store_name = None

        return CatalogInfo(
            name=catalog.name,
            created_at=catalog.created_at,
            updated_at=catalog.updated_at,
            description=catalog.description,
            feature_store_name=feature_store_name,
            online_store_name=online_store_name,
        )
