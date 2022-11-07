"""
DataService class
"""
from __future__ import annotations

from typing import Any, Optional, Union
from typing_extensions import Annotated

from bson import ObjectId
from pydantic import Field, parse_obj_as

from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_store import DataModel as BaseDataModel
from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.data import DataCreate, DataUpdate
from featurebyte.service.base_document import BaseDocumentService, DocumentUpdateSchema
from featurebyte.service.mixin import Document, DocumentCreateSchema

DataModel = Annotated[Union[EventDataModel, ItemDataModel], Field(discriminator="type")]


class TabularDataModel(EventDataModel):
    """
    Pseudo Data class to support multiple data types
    """

    def __new__(cls, **kwargs: Any) -> Any:
        return parse_obj_as(DataModel, kwargs)  # type: ignore[arg-type]


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
