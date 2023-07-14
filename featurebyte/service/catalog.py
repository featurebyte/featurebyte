"""
CatalogService class
"""
from __future__ import annotations

from featurebyte.models.catalog import CatalogModel
from featurebyte.schema.catalog import CatalogCreate, CatalogServiceUpdate
from featurebyte.service.base_document import BaseDocumentService


class CatalogService(BaseDocumentService[CatalogModel, CatalogCreate, CatalogServiceUpdate]):
    """
    CatalogService class
    """

    document_class = CatalogModel


class AllCatalogService(CatalogService):
    """
    AllCatalogService class
    """

    @property
    def is_catalog_specific(self) -> bool:
        return False
