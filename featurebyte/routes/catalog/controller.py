"""
Catalog API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.exception import DocumentDeletionError
from featurebyte.models.catalog import CatalogModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.catalog import CatalogList, CatalogServiceUpdate, CatalogUpdate
from featurebyte.schema.info import CatalogInfo
from featurebyte.service.catalog import CatalogService
from featurebyte.service.deployment import AllDeploymentService


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
    ):
        super().__init__(service=service)
        self.all_deployment_service = all_deployment_service

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
            data=CatalogServiceUpdate(**data.dict()),
            return_document=False,
        )
        return await self.get(document_id=catalog_id)

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

        await self.service.soft_delete(document_id=catalog_id)

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
        return CatalogInfo(
            name=catalog.name,
            created_at=catalog.created_at,
            updated_at=catalog.updated_at,
            description=catalog.description,
        )
