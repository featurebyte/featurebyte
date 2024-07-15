"""
This module contains mixin class(es) used in the service directory.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Generic, Optional, Type, TypeVar

from bson import ObjectId

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
)
from featurebyte.persistent.base import SortDir

GeneralT = TypeVar("GeneralT")
Document = TypeVar("Document", bound=FeatureByteBaseDocumentModel)
DocumentCreateSchema = TypeVar("DocumentCreateSchema", bound=FeatureByteBaseModel)
DEFAULT_PAGE_SIZE = 100


class OpsServiceMixin:
    """
    OpsServiceMixin class contains common operation methods used across different type of services
    """

    @staticmethod
    def include_object_id(
        document_ids: list[ObjectId] | list[PydanticObjectId], document_id: ObjectId
    ) -> list[ObjectId]:
        """
        Include document_id to the document_ids list

        Parameters
        ----------
        document_ids: list[ObjectId]
            List of document IDs
        document_id: ObjectId
            Document ID to be included

        Returns
        -------
        List of sorted document_ids
        """
        return sorted(set(document_ids + [document_id]))

    @staticmethod
    def exclude_object_id(
        document_ids: list[ObjectId] | list[PydanticObjectId], document_id: ObjectId
    ) -> list[ObjectId]:
        """
        Exclude document_id from the document_ids list

        Parameters
        ----------
        document_ids: list[ObjectId]
            List of document IDs
        document_id: ObjectId
            Document ID to be excluded

        Returns
        -------
        List of sorted document_ids
        """
        return sorted(set(ObjectId(doc_id) for doc_id in document_ids if doc_id != document_id))

    @staticmethod
    def conditional_return(document: GeneralT, condition: bool) -> Optional[GeneralT]:
        """
        Return output only if condition is True

        Parameters
        ----------
        document: GeneralT
            Document to be returned
        condition: bool
            Flag to control whether to return document

        Returns
        -------
        Optional[GeneralT]
        """
        if condition:
            return document
        return None


class GetOrCreateMixin(Generic[Document, DocumentCreateSchema]):
    """
    CreateOrGetMixin class contains method to create a new document if it does not exist when retrieving a document
    """

    document_class: Type[Document]
    document_create_class: Type[DocumentCreateSchema]

    @abstractmethod
    async def create_document(self, data: DocumentCreateSchema) -> Document:
        """
        Create document at persistent

        Parameters
        ----------
        data: DocumentCreateSchema
            Document creation payload object

        Returns
        -------
        Document
        """

    @abstractmethod
    async def list_documents_as_dict(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: list[tuple[str, SortDir]] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        List documents stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: list[tuple[str, SortDir]] | None
            Keys and directions used to sort the returning documents
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        dict[str, Any]
            List of documents fulfilled the filtering condition
        """

    async def get_or_create_document(self, name: str) -> Document:
        """
        Retrieve (or create a new one if it does not exist) a document by name

        Parameters
        ----------
        name: str
            Name used to retrieve the document

        Returns
        -------
        SemanticModel
        """
        documents = await self.list_documents_as_dict(query_filter={"name": name})
        if documents["data"]:
            return self.document_class(**documents["data"][0])
        return await self.create_document(data=self.document_create_class(name=name))
