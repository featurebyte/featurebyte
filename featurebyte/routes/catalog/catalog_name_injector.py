"""
Catalog name injector
"""

from __future__ import annotations

from typing import Any, List, Tuple

from featurebyte.models.base import PydanticObjectId
from featurebyte.service.catalog import CatalogService


class CatalogNameInjector:
    """
    Catalog name injector
    """

    def __init__(self, catalog_service: CatalogService):
        self.catalog_service = catalog_service

    async def add_name(
        self, catalog_id: PydanticObjectId, documents: List[dict[str, Any]]
    ) -> Tuple[str, List[dict[str, Any]]]:
        """
        Add catalog name to the documents.

        Parameters
        ----------
        catalog_id: PydanticObjectId
            Catalog id
        documents: List[dict[str, Any]]
            Documents to be updated

        Returns
        -------
        Tuple[str, List[dict[str, Any]]]
            catalog name, and update document groups
        """
        catalog = await self.catalog_service.get_document(catalog_id)
        catalog_name = catalog.name
        assert catalog_name is not None
        updated_group = []
        for document_group in documents:
            for document in document_group["data"]:
                assert document["catalog_id"] == catalog.id
                document["catalog_name"] = catalog_name
            updated_group.append(document_group)
        return catalog_name, updated_group
