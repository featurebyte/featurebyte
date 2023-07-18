"""
Catalog API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.models.catalog import CatalogModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.catalog import (
    CatalogCreate,
    CatalogList,
    CatalogServiceUpdate,
    CatalogUpdate,
)
from featurebyte.schema.info import CatalogInfo
from featurebyte.service.catalog import CatalogService


class CatalogController(
    BaseDocumentController[CatalogModel, CatalogService, CatalogList],
):
    """
    Catalog Controller
    """

    paginated_document_class = CatalogList

    async def create_catalog(
        self,
        data: CatalogCreate,
    ) -> CatalogModel:
        """
        Create Catalog at persistent

        Parameters
        ----------
        data: CatalogCreate
            Catalog creation payload

        Returns
        -------
        CatalogModel
            Newly created Catalog object
        """
        return await self.service.create_document(data)

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
