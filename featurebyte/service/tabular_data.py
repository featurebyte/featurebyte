"""
DataService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId

from featurebyte.models.feature_store import DataModel as BaseDataModel
from featurebyte.models.tabular_data import TabularDataModel
from featurebyte.schema.tabular_data import DataCreate, DataUpdate
from featurebyte.service.base_document import BaseDocumentService, DocumentUpdateSchema
from featurebyte.service.mixin import Document, DocumentCreateSchema


class DataService(BaseDocumentService[BaseDataModel, DataCreate, DataUpdate]):
    """
    DataService class
    """

    document_class = TabularDataModel

    async def create_document(
        self, data: DocumentCreateSchema, get_credential: Any = None
    ) -> Document:
        raise NotImplementedError

    async def update_document(
        self,
        document_id: ObjectId,
        data: DocumentUpdateSchema,
        exclude_none: bool = True,
        document: Optional[Document] = None,
        return_document: bool = True,
    ) -> Optional[Document]:
        raise NotImplementedError
