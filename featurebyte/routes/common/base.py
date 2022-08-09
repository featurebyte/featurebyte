"""
BaseController for API routes
"""
from __future__ import annotations

from typing import Any, Dict, Generic, Iterator, List, Literal, Optional, Type, TypeVar, cast

import copy
from http import HTTPStatus

import numpy as np
import pandas as pd
from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    UniqueConstraintResolutionSignature,
)
from featurebyte.models.persistent import (
    AuditActionType,
    AuditDocumentList,
    FieldValueHistory,
    QueryFilter,
)
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
    def to_class_name(cls, collection_name: str | None = None) -> str:
        """
        Class represents the underlying collection name

        Parameters
        ----------
        collection_name: str | None
            Collection name

        Returns
        -------
        str
        """
        collection_name = collection_name or cls.collection_name
        return "".join(elem.title() for elem in collection_name.split("_"))

    @classmethod
    def _format_document(cls, doc: dict[str, Any]) -> str:
        return ", ".join(f'{key}: "{value}"' for key, value in doc.items())

    @classmethod
    def get_conflict_message(
        cls,
        conflict_doc: dict[str, Any],
        conflict_signature: dict[str, Any],
        resolution_signature: UniqueConstraintResolutionSignature,
        class_name: str | None = None,
    ) -> str:
        """
        Get the conflict error message

        Parameters
        ----------
        conflict_doc: dict[str, Any]
            Existing document that causes conflict
        conflict_signature: dict[str, Any]
            Document used to represent conflict information
        resolution_signature: UniqueConstraintResolutionSignature
            Get method used to retrieved conflict object
        class_name: str | None
            Class used in the resolution statement

        Returns
        -------
        str
            Error message for conflict exception
        """
        resolution_statement = UniqueConstraintResolutionSignature.get_resolution_statement(
            resolution_signature=resolution_signature,
            class_name=class_name or cls.to_class_name(),
            document=conflict_doc,
        )
        return (
            f"{cls.to_class_name()} ({cls._format_document(conflict_signature)}) already exists. "
            f"Get the existing object by `{resolution_statement}`."
        )

    @classmethod
    async def check_document_unique_constraint(
        cls,
        persistent: Persistent,
        query_filter: dict[str, Any],
        conflict_signature: dict[str, Any],
        user_id: ObjectId | None,
        resolution_signature: UniqueConstraintResolutionSignature,
    ) -> None:
        """
        Check document creation conflict

        Parameters
        ----------
        persistent: Persistent
            Persistent object
        query_filter: dict[str, Any]
            Query filter that will be passed to persistent
        conflict_signature: dict[str, Any]
            Document representation that shows user the conflict fields
        user_id: ObjectId
            user_id
        resolution_signature: UniqueConstraintResolutionSignature
            Object retrieval option shows in error message.

        Raises
        ------
        HTTPException
            When there is a conflict with existing document(s) stored at the persistent
        """
        if query_filter:
            # this is temporary fix to make sure we handle tabular_source tuple properly.
            # after revising current document models by removing all tuple attributes, this will be fixed.
            for key, value in query_filter.items():
                if isinstance(value, tuple):
                    query_filter[key] = list(query_filter[key])

        conflict_doc = await persistent.find_one(
            collection_name=cls.collection_name,
            query_filter=query_filter,
            user_id=user_id,
        )
        if conflict_doc:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=cls.get_conflict_message(
                    conflict_doc=cast(Dict[str, Any], conflict_doc),
                    conflict_signature=conflict_signature,
                    resolution_signature=resolution_signature,
                ),
            )

    @classmethod
    def _get_field_path_value(cls, doc_dict: Any, field_path: Any) -> Any:
        if isinstance(doc_dict, list) and isinstance(field_path, int):
            return doc_dict[field_path]
        if field_path:
            return cls._get_field_path_value(doc_dict[field_path[0]], field_path[1:])
        return doc_dict

    @classmethod
    def get_unique_constraints(
        cls, document: FeatureByteBaseDocumentModel
    ) -> Iterator[tuple[dict[str, Any], dict[str, Any], UniqueConstraintResolutionSignature]]:
        """
        Generator used to extract uniqueness constraints from document model setting

        Parameters
        ----------
        document: FeatureByteBaseDocumentModel
            Document contains information to construct query filter & conflict signature

        Returns
        -------
        Iterator[dict[str, Any], dict[str, Any], UniqueConstraintResolutionSignature]
        """
        doc_dict = document.dict(by_alias=True)
        for constraint in cls.document_class.Settings.unique_constraints:
            query_filter = {field: doc_dict[field] for field in constraint.fields}
            conflict_signature = {
                name: cls._get_field_path_value(doc_dict, fields)
                for name, fields in constraint.conflict_fields_signature.items()
            }
            yield query_filter, conflict_signature, constraint.resolution_signature

    @classmethod
    async def check_document_unique_constraints(
        cls,
        persistent: Persistent,
        user_id: ObjectId,
        document: FeatureByteBaseDocumentModel,
    ) -> None:
        """
        Check document uniqueness constraints given document

        Parameters
        ----------
        persistent: Persistent
            persistent object
        user_id: ObjectId
            user id
        document: FeatureByteBaseDocumentModel
            document to be checked
        """
        for query_filter, conflict_signature, resolution_signature in cls.get_unique_constraints(
            document
        ):
            await cls.check_document_unique_constraint(
                persistent=persistent,
                query_filter=query_filter,
                conflict_signature=conflict_signature,
                user_id=user_id,
                resolution_signature=resolution_signature,
            )

    @classmethod
    async def get_document(
        cls,
        user: Any,
        persistent: Persistent,
        collection_name: str,
        document_id: ObjectId,
        exception_status_code: int = HTTPStatus.NOT_FOUND,
        exception_detail: str | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve document dictionary given collection and document id (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Persistent that the document will be saved to
        collection_name: str
            Collection name
        document_id: ObjectId
            Document ID
        exception_status_code: HTTPStatus
            Status code used in raising exception
        exception_detail: str | None
            Exception detail message

        Returns
        -------
        dict[str, Any]
        """
        query_filter = {"_id": ObjectId(document_id), "user_id": user.id}
        document = await persistent.find_one(
            collection_name=collection_name, query_filter=query_filter, user_id=user.id
        )
        if document is None:
            class_name = cls.to_class_name(collection_name)
            exception_detail = exception_detail or (
                f'{class_name} (id: "{document_id}") not found. '
                f"Please save the {class_name} object first."
            )
            raise HTTPException(
                status_code=exception_status_code,
                detail=exception_detail,
            )
        return cast(Dict[str, Any], document)

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
        document = await cls.get_document(
            user=user,
            persistent=persistent,
            collection_name=cls.collection_name,
            document_id=document_id,
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
                user_id=user.id,
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
        search: str | None = None,
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
        search: str | None
            Search term (not supported)

        Returns
        -------
        AuditDocumentList
            List of documents fulfilled the filtering condition
        """
        query_filter = copy.deepcopy(query_filter) if query_filter else {}
        query_filter.update({"user_id": user.id})
        if search:
            query_filter["$text"] = {"$search": search}

        try:
            docs, total = await persistent.get_audit_logs(
                collection_name=cls.collection_name,
                document_id=document_id,
                query_filter=query_filter,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
                page_size=page_size,
            )
            return AuditDocumentList(page=page, page_size=page_size, total=total, data=list(docs))
        except NotImplementedError as exc:
            raise HTTPException(
                status_code=HTTPStatus.NOT_IMPLEMENTED, detail="Query not supported."
            ) from exc

    @classmethod
    def _get_field_history(
        cls, field: str, audit_docs: List[Dict[str, Any]]
    ) -> List[FieldValueHistory]:
        history: List[FieldValueHistory] = []
        for doc in audit_docs:
            if doc["action_type"] not in {AuditActionType.INSERT, AuditActionType.UPDATE}:
                # skip action_type other than insert & update
                continue
            current_values, previous_values = doc["current_values"], doc["previous_values"]
            current_value = current_values.get(field, np.nan)
            previous_value = previous_values.get(field, np.nan)
            if doc["action_type"] == AuditActionType.INSERT:
                # always insert an initial record (display np.nan if the value is missing)
                history.append(
                    FieldValueHistory(
                        created_at=current_values.get("created_at"),
                        value=current_values.get(field, np.nan),
                    )
                )
            elif doc["action_type"] == AuditActionType.UPDATE:
                # previous null, current not null => a new field is introduced
                # previous null, current null => not related
                # previous not null, current null => an existing field is removed
                # previous not null, current not null => update an existing field
                if pd.isnull(current_value) and pd.isnull(previous_value):
                    continue

                # for the other 3 cases, just need to insert current updated value (np.nan for field removal)
                history.append(
                    FieldValueHistory(
                        created_at=current_values.get("updated_at"),
                        value=current_value,
                    ),
                )
        return list(reversed(history))

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
        query_filter = {
            "user_id": user.id,
        }

        try:
            docs, _ = await persistent.get_audit_logs(
                collection_name=cls.collection_name,
                document_id=document_id,
                query_filter={**query_filter},
                sort_by="action_at",
                sort_dir="asc",
            )
            return cls._get_field_history(field=field, audit_docs=cast(List[Dict[str, Any]], docs))

        except NotImplementedError as exc:
            raise HTTPException(
                status_code=HTTPStatus.NOT_IMPLEMENTED, detail="Query not supported."
            ) from exc
