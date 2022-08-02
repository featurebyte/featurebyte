"""
BaseController for API routes
"""
from __future__ import annotations

from typing import Any, Generic, Literal, Optional, Type, TypeVar, cast

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.persistent.base import Persistent
from featurebyte.routes.common.schema import PaginationMixin

Document = TypeVar("Document", bound=FeatureByteBaseDocumentModel)
PaginatedDocument = TypeVar("PaginatedDocument", bound=PaginationMixin)


class BaseController(Generic[Document, PaginatedDocument]):
    """
    BaseController for API routes
    """

    collection_name: str = ""
    document_class: Type[FeatureByteBaseDocumentModel] = FeatureByteBaseDocumentModel
    paginated_document_class: Type[PaginationMixin] = PaginationMixin

    @classmethod
    def class_name(cls) -> str:
        """
        Class represents the underlying collection name

        Returns
        -------
        str
        """
        return "".join(elem.title() for elem in cls.collection_name.split("_"))

    @classmethod
    def object_name(cls) -> str:
        """
        Object that is constructed by the class represents the underlying collection

        Returns
        -------
        str
        """
        return cls.collection_name

    @classmethod
    async def get(
        cls,
        user: Any,
        persistent: Persistent,
        document_id: ObjectId,
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

        Returns
        -------
        Document

        Raises
        ------
        HTTPException
            If the object not found
        """
        query_filter = {"_id": ObjectId(document_id), "user_id": user.id}
        document = await persistent.find_one(
            collection_name=cls.collection_name, query_filter=query_filter
        )
        if document is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=(
                    f'{cls.class_name()} ({cls.object_name()}.id: "{document_id}") not found! '
                    f"Please save the {cls.class_name()} object first."
                ),
            )
        return cast(Document, cls.document_class(**document))

    @classmethod
    async def list(
        cls,
        user: Any,
        persistent: Persistent,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        name: Optional[str] = None,
        search: str | None = None,
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
        name: str | None
            Document name used to filter the documents
        search: str | None
            Search term (not supported)

        Returns
        -------
        dict[str, Any]
            List of documents fulfilled the filtering condition
        """
        query_filter = {"user_id": user.id}
        if name is not None:
            query_filter["name"] = name
        if search:
            query_filter["$text"] = {"$search": search}

        try:
            docs, total = await persistent.find(
                collection_name=cls.collection_name,
                query_filter=query_filter,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
                page_size=page_size,
            )
            return cast(
                PaginatedDocument,
                cls.paginated_document_class(
                    page=page, page_size=page_size, total=total, data=list(docs)
                ),
            )
        except NotImplementedError as exc:
            raise HTTPException(
                status_code=HTTPStatus.NOT_IMPLEMENTED, detail="Query not supported."
            ) from exc
