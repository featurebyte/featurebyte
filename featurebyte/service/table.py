"""
TableService class
"""

from __future__ import annotations

from typing import Optional

from bson import ObjectId

from featurebyte.models.feature_store import TableModel as BaseDataModel
from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.schema.table import TableCreate, TableServiceUpdate
from featurebyte.service.base_document import BaseDocumentService, DocumentUpdateSchema
from featurebyte.service.mixin import Document, DocumentCreateSchema


class TableService(BaseDocumentService[BaseDataModel, TableCreate, TableServiceUpdate]):
    """
    TableService class
    """

    document_class = ProxyTableModel

    @property
    def is_catalog_specific(self) -> bool:
        return True

    async def create_document(self, data: DocumentCreateSchema) -> Document:
        raise NotImplementedError

    async def update_document(
        self,
        document_id: ObjectId,
        data: DocumentUpdateSchema,
        exclude_none: bool = True,
        document: Optional[Document] = None,
        return_document: bool = True,
        skip_block_modification_check: bool = False,
        populate_remote_attributes: bool = True,
    ) -> Optional[Document]:
        raise NotImplementedError


class AllTableService(TableService):
    """
    TableService class
    """

    @property
    def is_catalog_specific(self) -> bool:
        return False
