"""
BaseController for API routes
"""
from __future__ import annotations

from typing import Any, Generic, List, Literal, Optional, Type, TypeVar, cast

from bson.objectid import ObjectId

from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.persistent import AuditDocumentList, FieldValueHistory, QueryFilter
from featurebyte.models.relationship import Parent
from featurebyte.routes.common.schema import PaginationMixin
from featurebyte.service.base_document import (
    BaseDocumentService,
    Document,
    GetInfoServiceMixin,
    InfoDocument,
)
from featurebyte.service.relationship import RelationshipService

PaginatedDocument = TypeVar("PaginatedDocument", bound=PaginationMixin)
ParentT = TypeVar("ParentT", bound=Parent)


class BaseDocumentController(Generic[Document, PaginatedDocument]):
    """
    BaseDocumentController for API routes
    """

    paginated_document_class: Type[PaginationMixin] = PaginationMixin

    def __init__(self, service: BaseDocumentService[FeatureByteBaseDocumentModel]):
        self.service = service

    async def get(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
    ) -> Document:
        """
        Retrieve document given document id (GitDB or MongoDB)

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        exception_detail: str | None
            Exception detail message

        Returns
        -------
        Document

        Raises
        ------
        HTTPException
            If the object not found
        """
        document = await self.service.get_document(
            document_id=document_id,
            exception_detail=exception_detail,
        )
        return cast(Document, document)

    async def list(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> PaginatedDocument:
        """
        List documents stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: str | None
            Key used to sort the returning documents
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        dict[str, Any]
            List of documents fulfilled the filtering condition
        """
        document_data = await self.service.list_documents(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **kwargs,
        )
        return cast(PaginatedDocument, self.paginated_document_class(**document_data))

    async def list_audit(
        self,
        document_id: ObjectId,
        query_filter: Optional[QueryFilter] = None,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> AuditDocumentList:
        """
        List audit records stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Persistent to retrieve audit docs from
        document_id: ObjectId
            ID of document to retrieve
        query_filter: Optional[QueryFilter]
            Filter to apply on results
        page: int
            Page number
        page_size: int
            Number of items per page
        sort_by: str | None
            Key used to sort the returning documents
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        AuditDocumentList
            List of documents fulfilled the filtering condition
        """
        document_data = await self.service.list_document_audits(
            document_id=document_id,
            query_filter=query_filter,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **kwargs,
        )
        return AuditDocumentList(**document_data)

    async def list_field_history(
        self,
        document_id: ObjectId,
        field: str,
    ) -> List[FieldValueHistory]:
        """
        List historical values for a field in a document

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Persistent to retrieve audit docs from
        document_id: ObjectId
            ID of document to retrieve
        field: str
            Name of field to get history for

        Returns
        -------
        List[FieldValueHistory]
            List of historical values for a field in the document
        """
        document_data = await self.service.list_document_field_history(
            document_id=document_id, field=field
        )
        return document_data


class GetInfoControllerMixin(Generic[InfoDocument]):
    """
    GetInfoControllerMixin contains method to retrieve document info
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, service: GetInfoServiceMixin[InfoDocument]):
        self.service = service

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> InfoDocument:
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
        info_document = await self.service.get_info(document_id=document_id, verbose=verbose)
        return info_document


class RelationshipMixin(Generic[Document, ParentT]):
    """
    RelationshipMixin contains methods to add & remove parent relationship
    """

    relationship_service: RelationshipService

    async def create_relationship(self, data: ParentT, child_id: ObjectId) -> Document:
        """
        Create relationship at persistent

        Parameters
        ----------
        data: ParentT
            Parent payload
        child_id: ObjectId
            Child entity ID

        Returns
        -------
        Document
            Document model with newly created relationship
        """
        document = await self.relationship_service.add_relationship(parent=data, child_id=child_id)
        return cast(Document, document)

    async def remove_relationship(
        self,
        parent_id: ObjectId,
        child_id: ObjectId,
    ) -> Document:
        """
        Remove relationship at persistent

        Parameters
        ----------
        parent_id: ObjectId
            Parent ID
        child_id: ObjectId
            Child ID

        Returns
        -------
        Document
            Document model with specified relationship get removed
        """
        document = await self.relationship_service.remove_relationship(
            parent_id=parent_id,
            child_id=child_id,
        )
        return cast(Document, document)
