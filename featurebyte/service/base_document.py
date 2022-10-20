"""
BaseService class
"""
from __future__ import annotations

from typing import Any, Dict, Generic, Iterator, List, Literal, Optional, Type, TypeVar

import copy
from abc import abstractmethod

import numpy as np
import pandas as pd
from bson.objectid import ObjectId

from featurebyte.common.dict_util import get_field_path_value
from featurebyte.exception import DocumentConflictError, DocumentNotFoundError
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    UniqueConstraintResolutionSignature,
    VersionIdentifier,
)
from featurebyte.models.persistent import AuditActionType, FieldValueHistory, QueryFilter
from featurebyte.persistent.base import Persistent
from featurebyte.schema.common.base import BaseInfo
from featurebyte.service.mixin import OpsServiceMixin

Document = TypeVar("Document", bound=FeatureByteBaseDocumentModel)
InfoDocument = TypeVar("InfoDocument", bound=BaseInfo)


class BaseDocumentService(Generic[Document], OpsServiceMixin):
    """
    BaseService class
    """

    document_class: Type[Document]

    def __init__(self, user: Any, persistent: Persistent):
        self.user = user
        self.persistent = persistent

    @property
    def collection_name(self) -> str:
        """
        Collection name

        Returns
        -------
        Collection name
        """
        return self.document_class.collection_name()

    @property
    def class_name(self) -> str:
        """
        API Object Class name used to represent the underlying collection name

        Returns
        -------
        camel case collection name
        """
        return "".join(elem.title() for elem in self.collection_name.split("_"))

    def _construct_get_query_filter(self, document_id: ObjectId, **kwargs: Any) -> QueryFilter:
        """
        Construct query filter used in get route

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        QueryFilter
        """
        _ = self, kwargs
        return {"_id": ObjectId(document_id)}

    async def get_document(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
        **kwargs: Any,
    ) -> Document:
        """
        Retrieve document dictionary given document id

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        exception_detail: str | None
            Exception detail message
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        Document

        Raises
        ------
        DocumentNotFoundError
            If the requested document not found
        """
        query_filter = self._construct_get_query_filter(document_id=document_id, **kwargs)
        document_dict = await self.persistent.find_one(
            collection_name=self.collection_name,
            query_filter=query_filter,
            user_id=self.user.id,
        )
        if document_dict is None:
            exception_detail = exception_detail or (
                f'{self.class_name} (id: "{document_id}") not found. Please save the {self.class_name} object first.'
            )
            raise DocumentNotFoundError(exception_detail)
        return self.document_class(**document_dict)

    def _construct_list_query_filter(
        self, query_filter: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> QueryFilter:
        """
        Construct query filter used in list route

        Parameters
        ----------
        query_filter: Optional[Dict[str, Any]]
            Query filter to use as starting point
        kwargs: Any
            Keyword arguments passed to the list controller

        Returns
        -------
        QueryFilter
        """
        _ = self
        if not query_filter:
            output = {}
        else:
            output = copy.deepcopy(query_filter)
        if kwargs.get("name"):
            output["name"] = kwargs["name"]
        if kwargs.get("search"):
            output["$text"] = {"$search": kwargs["search"]}
        return output

    async def list_documents(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
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
        query_filter = self._construct_list_query_filter(**kwargs)
        docs, total = await self.persistent.find(
            collection_name=self.collection_name,
            query_filter=query_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
            user_id=self.user.id,
        )
        return {"page": page, "page_size": page_size, "total": total, "data": list(docs)}

    def _construct_list_audit_query_filter(
        self, query_filter: Optional[QueryFilter], **kwargs: Any
    ) -> QueryFilter:
        """
        Construct query filter used in list audit route

        Parameters
        ----------
        query_filter: Optional[QueryFilter]
            Input query filter
        kwargs: Any
            Keyword arguments passed to the list controller

        Returns
        -------
        QueryFilter
        """
        _ = self
        query_filter = copy.deepcopy(query_filter) if query_filter else {}
        if kwargs.get("search"):
            query_filter["$text"] = {"$search": kwargs["search"]}
        return query_filter

    async def list_document_audits(
        self,
        document_id: ObjectId,
        query_filter: Optional[QueryFilter] = None,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        List audit records stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
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
        dict[str, Any]
            List of documents fulfilled the filtering condition
        """
        query_filter = self._construct_list_audit_query_filter(query_filter=query_filter, **kwargs)
        docs, total = await self.persistent.get_audit_logs(
            collection_name=self.collection_name,
            document_id=document_id,
            query_filter=query_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )
        return {"page": page, "page_size": page_size, "total": total, "data": list(docs)}

    @classmethod
    def _get_field_history(
        cls, field: str, audit_docs: List[Dict[str, Any]]
    ) -> List[FieldValueHistory]:
        """
        Construct list of audit history given field

        Parameters
        ----------
        field: str
            Field name
        audit_docs: List[Dict[str, Any]]
            List of audit history retrieved from persistent

        Returns
        -------
        List[FieldValueHistory]
        """
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

    async def list_document_field_history(
        self, document_id: ObjectId, field: str
    ) -> list[FieldValueHistory]:
        """
        List historical values for a field in a document

        Parameters
        ----------
        document_id: ObjectId
            ID of document to retrieve
        field: str
            Name of field to get history for

        Returns
        -------
        List[FieldValueHistory]
            List of historical values for a field in the document
        """
        audit_data = await self.list_document_audits(
            document_id=document_id,
            query_filter={},
            sort_by="action_at",
            sort_dir="asc",
        )
        return self._get_field_history(field=field, audit_docs=audit_data["data"])

    def _get_conflict_message(
        self,
        conflict_doc: dict[str, Any],
        conflict_signature: dict[str, Any],
        resolution_signature: Optional[UniqueConstraintResolutionSignature],
    ) -> str:
        """
        Get the conflict error message

        Parameters
        ----------
        conflict_doc: dict[str, Any]
            Existing document that causes conflict
        conflict_signature: dict[str, Any]
            Document used to represent conflict information
        resolution_signature: Optional[UniqueConstraintResolutionSignature]
            Get method used to retrieved conflict object

        Returns
        -------
        str
            Error message for conflict exception
        """
        formatted_conflict_signature = ", ".join(
            f'{key}: "{value}"' for key, value in conflict_signature.items()
        )
        message = f"{self.class_name} ({formatted_conflict_signature}) already exists."
        if resolution_signature:
            if (
                resolution_signature
                in UniqueConstraintResolutionSignature.get_existing_object_type()
            ):
                resolution_statement = UniqueConstraintResolutionSignature.get_resolution_statement(
                    resolution_signature=resolution_signature,
                    class_name=self.class_name,
                    document=conflict_doc,
                )
                message += f" Get the existing object by `{resolution_statement}`."
            if resolution_signature == UniqueConstraintResolutionSignature.RENAME:
                message += (
                    f' Please rename object (name: "{conflict_doc["name"]}") to something else.'
                )
        return message

    async def _check_document_unique_constraint(
        self,
        query_filter: dict[str, Any],
        conflict_signature: dict[str, Any],
        resolution_signature: UniqueConstraintResolutionSignature | None,
    ) -> None:
        """
        Check document creation conflict

        Parameters
        ----------
        query_filter: dict[str, Any]
            Query filter that will be passed to persistent
        conflict_signature: dict[str, Any]
            Document representation that shows user the conflict fields
        resolution_signature: UniqueConstraintResolutionSignature | None
            Object retrieval option shows in error message.

        Raises
        ------
        DocumentConflictError
            When there is a conflict with existing document(s) stored at the persistent
        """
        if query_filter:
            # this is temporary fix to make sure we handle tabular_source tuple properly.
            # after revising current document models by removing all tuple attributes, this will be fixed.
            for key, value in query_filter.items():
                if isinstance(value, tuple):
                    query_filter[key] = list(query_filter[key])

        conflict_doc = await self.persistent.find_one(
            collection_name=self.collection_name,
            query_filter=query_filter,
            user_id=self.user.id,
        )
        if conflict_doc:
            exception_detail = self._get_conflict_message(
                conflict_doc=dict(conflict_doc),
                conflict_signature=conflict_signature,
                resolution_signature=resolution_signature,
            )
            raise DocumentConflictError(exception_detail)

    @classmethod
    def _get_unique_constraints(
        cls, document: FeatureByteBaseDocumentModel
    ) -> Iterator[
        tuple[dict[str, Any], dict[str, Any], Optional[UniqueConstraintResolutionSignature]]
    ]:
        """
        Generator used to extract uniqueness constraints from document model setting

        Parameters
        ----------
        document: FeatureByteBaseDocumentModel
            Document contains information to construct query filter & conflict signature

        Yields
        ------
        Iterator[dict[str, Any], dict[str, Any], Optional[UniqueConstraintResolutionSignature]]
            List of tuples used to construct unique constraint validations
        """
        doc_dict = document.dict(by_alias=True)
        for constraint in cls.document_class.Settings.unique_constraints:
            query_filter = {field: doc_dict[field] for field in constraint.fields}
            conflict_signature = {
                name: get_field_path_value(doc_dict, fields)
                for name, fields in constraint.conflict_fields_signature.items()
            }
            if conflict_signature.get("version"):
                conflict_signature["version"] = VersionIdentifier(
                    **conflict_signature["version"]
                ).to_str()
            yield query_filter, conflict_signature, constraint.resolution_signature

    async def _check_document_unique_constraints(
        self, document: FeatureByteBaseDocumentModel
    ) -> None:
        """
        Check document uniqueness constraints given document

        Parameters
        ----------
        document: FeatureByteBaseDocumentModel
            document to be checked
        """
        for query_filter, conflict_signature, resolution_signature in self._get_unique_constraints(
            document
        ):
            await self._check_document_unique_constraint(
                query_filter=query_filter,
                conflict_signature=conflict_signature,
                resolution_signature=resolution_signature,
            )

    @abstractmethod
    async def create_document(
        self, data: FeatureByteBaseModel, get_credential: Any = None
    ) -> Document:
        """
        Create document at persistent

        Parameters
        ----------
        data: FeatureByteBaseModel
            Document creation payload object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        Document
        """

    @abstractmethod
    async def update_document(
        self,
        document_id: ObjectId,
        data: FeatureByteBaseModel,
        exclude_none: bool = True,
        document: Optional[FeatureByteBaseDocumentModel] = None,
        return_document: bool = True,
    ) -> Optional[Document]:
        """
        Update document at persistent

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        data: FeatureByteBaseModel
            Document update payload object
        exclude_none: bool
            Whether to exclude None value(s) from the data
        document: Optional[FeatureByteBaseDocumentModel]
            Document to be updated (when provided, this method won't query persistent for retrieval)
        return_document: bool
            Whether to make additional query to retrieval updated document & return

        Returns
        -------
        Optional[Document]
        """


class GetInfoServiceMixin(Generic[InfoDocument]):
    """
    GetInfoServiceMixin contains method to retrieve document info
    """

    # pylint: disable=too-few-public-methods

    @abstractmethod
    async def get_info(self, document_id: ObjectId, verbose: bool) -> InfoDocument:
        """
        Retrieve document related info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control info verbose level

        Returns
        -------
        InfoDocument
        """
