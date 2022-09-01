"""
BaseController for API routes
"""
from __future__ import annotations

from typing import Any, AsyncIterator, Generic, List, Literal, Optional, Type, TypeVar, cast

from contextlib import asynccontextmanager
from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.exception import (
    DocumentConflictError,
    DocumentInconsistencyError,
    DocumentNotFoundError,
    DocumentUpdateError,
)
from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.persistent import AuditDocumentList, FieldValueHistory, QueryFilter
from featurebyte.persistent.base import Persistent
from featurebyte.routes.common.schema import PaginationMixin
from featurebyte.service.base_document import BaseDocumentService, Document
from featurebyte.service.document_info import DocumentInfoService

PaginatedDocument = TypeVar("PaginatedDocument", bound=PaginationMixin)


class BaseDocumentController(Generic[Document, PaginatedDocument]):
    """
    BaseController for API routes
    """

    paginated_document_class: Type[PaginationMixin] = PaginationMixin
    document_service_class: Type[BaseDocumentService[FeatureByteBaseDocumentModel]]

    @classmethod
    @asynccontextmanager
    async def _creation_context(cls) -> AsyncIterator[None]:
        try:
            yield
        except (DocumentNotFoundError, DocumentInconsistencyError, DocumentUpdateError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=str(exc)
            ) from exc
        except DocumentConflictError as exc:
            raise HTTPException(status_code=HTTPStatus.CONFLICT, detail=str(exc)) from exc

    @classmethod
    @asynccontextmanager
    async def _update_context(cls) -> AsyncIterator[None]:
        try:
            yield
        except DocumentNotFoundError as exc:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(exc)) from exc
        except DocumentUpdateError as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=str(exc)
            ) from exc
        except DocumentConflictError as exc:
            raise HTTPException(status_code=HTTPStatus.CONFLICT, detail=str(exc)) from exc

    @classmethod
    async def get(
        cls,
        user: Any,
        persistent: Persistent,
        document_id: ObjectId,
        exception_detail: str | None = None,
    ) -> Document:
        """
        Retrieve document given document id (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Persistent that the document will be saved to
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
        try:
            document = await cls.document_service_class(
                user=user, persistent=persistent
            ).get_document(
                document_id=document_id,
                exception_detail=exception_detail,
            )
            return cast(Document, document)
        except DocumentNotFoundError as exc:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(exc)) from exc

    @classmethod
    async def list(
        cls,
        user: Any,
        persistent: Persistent,
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
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Persistent that the document will be saved to
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
        try:
            document_data = await cls.document_service_class(
                user=user, persistent=persistent
            ).list_documents(
                page=page,
                page_size=page_size,
                sort_by=sort_by,
                sort_dir=sort_dir,
                **kwargs,
            )
        except NotImplementedError as exc:
            raise HTTPException(
                status_code=HTTPStatus.NOT_IMPLEMENTED, detail="Query not supported."
            ) from exc

        return cast(PaginatedDocument, cls.paginated_document_class(**document_data))

    @classmethod
    async def list_audit(
        cls,
        user: Any,
        persistent: Persistent,
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
        try:
            document_data = await cls.document_service_class(
                user=user, persistent=persistent
            ).list_document_audits(
                document_id=document_id,
                query_filter=query_filter,
                page=page,
                page_size=page_size,
                sort_by=sort_by,
                sort_dir=sort_dir,
                **kwargs,
            )
        except NotImplementedError as exc:
            raise HTTPException(
                status_code=HTTPStatus.NOT_IMPLEMENTED, detail="Query not supported."
            ) from exc

        return AuditDocumentList(**document_data)

    @classmethod
    async def list_field_history(
        cls,
        user: Any,
        persistent: Persistent,
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
        try:
            document_data = await cls.document_service_class(
                user=user, persistent=persistent
            ).list_document_field_history(document_id=document_id, field=field)
            return document_data
        except NotImplementedError as exc:
            raise HTTPException(
                status_code=HTTPStatus.NOT_IMPLEMENTED, detail="Query not supported."
            ) from exc

    @classmethod
    async def get_info(
        cls, user: Any, persistent: Persistent, document_id: ObjectId, verbose: bool = True
    ) -> dict[str, Any]:
        """
        Construct info based on the given document_id
        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Persistent to retrieve audit docs from
        document_id: ObjectId
            ID of document to retrieve
        verbose: bool
            Control verbose level of the info
        Returns
        -------
        dict[str, Any]
        """
        document_info_service = DocumentInfoService(user=user, persistent=persistent)
        return await document_info_service.get_info(
            collection_name=cls.document_service_class.document_class.collection_name(),
            document_id=document_id,
            verbose=verbose,
        )
