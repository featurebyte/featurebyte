"""
BaseService class
"""
from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Generic, Iterator, List, Optional, Type, TypeVar, Union

import copy

import numpy as np
import pandas as pd
from bson.objectid import ObjectId

from featurebyte.common.dict_util import get_field_path_value
from featurebyte.exception import (
    DocumentConflictError,
    DocumentNotFoundError,
    QueryNotSupportedError,
)
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteWorkspaceBaseDocumentModel,
    UniqueConstraintResolutionSignature,
    VersionIdentifier,
)
from featurebyte.models.persistent import AuditActionType, FieldValueHistory, QueryFilter
from featurebyte.persistent.base import Persistent
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, BaseInfo
from featurebyte.service.mixin import Document, DocumentCreateSchema, OpsServiceMixin, SortDir

DocumentUpdateSchema = TypeVar("DocumentUpdateSchema", bound=BaseDocumentServiceUpdateSchema)
InfoDocument = TypeVar("InfoDocument", bound=BaseInfo)


class BaseDocumentService(
    Generic[Document, DocumentCreateSchema, DocumentUpdateSchema], OpsServiceMixin
):
    """
    BaseDocumentService class is responsible to perform CRUD of the underlying persistent
    collection. It will perform model level validation before writing to persistent and after
    reading from the persistent.
    """

    document_class: Type[Document]

    def __init__(self, user: Any, persistent: Persistent, workspace_id: ObjectId):
        self.user = user
        self.persistent = persistent
        self.workspace_id = workspace_id

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

    @property
    def is_workspace_specific(self) -> bool:
        """
        Whether service operates on workspace specific documents

        Returns
        -------
        bool
        """
        return issubclass(self.document_class, FeatureByteWorkspaceBaseDocumentModel)

    @staticmethod
    def _extract_additional_creation_kwargs(data: DocumentCreateSchema) -> dict[str, Any]:
        """
        Extract additional document creation from document creation schema

        Parameters
        ----------
        data: DocumentCreateSchema
            Document creation schema

        Returns
        -------
        dict[str, Any]
        """
        _ = data
        return {}

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
        kwargs = self._extract_additional_creation_kwargs(data)
        if self.is_workspace_specific:
            kwargs = {**kwargs, "workspace_id": self.workspace_id}

        document = self.document_class(
            **{
                **data.json_dict(),
                **kwargs,
                "user_id": self.user.id,
            },
        )

        # check any conflict with existing documents
        await self._check_document_unique_constraints(
            document=document,
            document_class=self.document_class,
        )
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document.dict(by_alias=True),
            user_id=self.user.id,
        )
        assert insert_id == document.id
        return await self.get_document(document_id=insert_id)

    def _construct_get_query_filter(
        self, document_id: ObjectId, use_raw_query_filter: bool = False, **kwargs: Any
    ) -> QueryFilter:
        """
        Construct query filter used in get route

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        use_raw_query_filter: bool
            Use only provided query filter
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        QueryFilter
        """
        _ = self, kwargs
        kwargs = {"_id": ObjectId(document_id)}
        if use_raw_query_filter:
            return kwargs

        # inject workspace_id into filter if document is workspace specific
        if self.is_workspace_specific:
            kwargs = {**kwargs, "workspace_id": self.workspace_id}
        return kwargs

    async def get_document(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
        use_raw_query_filter: bool = False,
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
        use_raw_query_filter: bool
            Use only provided query filter
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
        query_filter = self._construct_get_query_filter(
            document_id=document_id, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
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
        self,
        query_filter: Optional[Dict[str, Any]] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> QueryFilter:
        """
        Construct query filter used in list route

        Parameters
        ----------
        query_filter: Optional[Dict[str, Any]]
            Query filter to use as starting point
        use_raw_query_filter: bool
            Use only provided query filter
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

        if use_raw_query_filter:
            return output
        if kwargs.get("name"):
            output["name"] = kwargs["name"]
        if kwargs.get("version"):
            output["version"] = kwargs["version"]
        if kwargs.get("search"):
            output["$text"] = {"$search": kwargs["search"]}
        # inject workspace_id into filter if document is workspace specific
        if self.is_workspace_specific:
            output["workspace_id"] = self.workspace_id

        return output

    async def list_documents(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: SortDir = "desc",
        use_raw_query_filter: bool = False,
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
        sort_dir: SortDir
            Sorting the returning documents in ascending order or descending order
        use_raw_query_filter: bool
            Use only provided query filter
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        dict[str, Any]
            List of documents fulfilled the filtering condition

        Raises
        ------
        QueryNotSupportedError
            If the persistent query is not supported
        """
        query_filter = self._construct_list_query_filter(
            use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        try:
            docs, total = await self.persistent.find(
                collection_name=self.collection_name,
                query_filter=query_filter,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
                page_size=page_size,
                user_id=self.user.id,
            )
        except NotImplementedError as exc:
            raise QueryNotSupportedError from exc
        return {"page": page, "page_size": page_size, "total": total, "data": list(docs)}

    async def list_documents_iterator(
        self,
        query_filter: Dict[str, Any],
        page_size: int = 1000,
        use_raw_query_filter: bool = False,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        List documents iterator to retrieve all the results based on given document service & query filter

        Parameters
        ----------
        query_filter: Dict[str, Any]
            Query filter
        page_size: int
            Page size
        use_raw_query_filter: bool
            Use only provided query filter

        Yields
        ------
        AsyncIterator[Dict[str, Any]]
            List query output
        """
        to_iterate, page = True, 1

        while to_iterate:
            list_results = await self.list_documents(
                page=page,
                page_size=page_size,
                query_filter=query_filter,
                use_raw_query_filter=use_raw_query_filter,
            )
            for doc in list_results["data"]:
                yield doc

            to_iterate = bool(list_results["total"] > (page * page_size))
            page += 1

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
        sort_dir: SortDir = "desc",
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
        sort_dir: SortDir
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
            page=1,
            page_size=0,
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
        # for workspace specific document type constraint uniqueness checking to the workspace
        if self.is_workspace_specific:
            query_filter = {**query_filter, "workspace_id": self.workspace_id}

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

    @staticmethod
    def _get_unique_constraints(
        document: FeatureByteBaseDocumentModel,
        document_class: Union[Type[Document], Type[DocumentUpdateSchema]],
        original_document: Optional[FeatureByteBaseDocumentModel],
    ) -> Iterator[
        tuple[dict[str, Any], dict[str, Any], Optional[UniqueConstraintResolutionSignature]]
    ]:
        """
        Generator used to extract uniqueness constraints from document model setting

        Parameters
        ----------
        document: FeatureByteBaseDocumentModel
            Document contains information to construct query filter & conflict signature
        document_class: Union[Type[Document], Type[DocumentUpdateSchema]]
            Document class used to retrieve unique constraints
        original_document: Optional[FeatureByteBaseDocumentModel]
            Original document before update (provide when checking document update conflict)

        Yields
        ------
        Iterator[dict[str, Any], dict[str, Any], Optional[UniqueConstraintResolutionSignature]]
            List of tuples used to construct unique constraint validations
        """
        doc_dict = document.dict(by_alias=True)
        original_dict = original_document.dict(by_alias=True) if original_document else None
        for constraint in document_class.Settings.unique_constraints:
            query_filter = {field: doc_dict[field] for field in constraint.fields}
            if original_dict:
                # skip checking if the original document value are the same with the updated one
                original_value = {field: original_dict[field] for field in constraint.fields}
                if original_value == query_filter:
                    continue

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
        self,
        document: FeatureByteBaseDocumentModel,
        document_class: Optional[Union[Type[Document], Type[DocumentUpdateSchema]]] = None,
        original_document: Optional[FeatureByteBaseDocumentModel] = None,
    ) -> None:
        """
        Check document uniqueness constraints given document

        Parameters
        ----------
        document: FeatureByteBaseDocumentModel
            document to be checked
        document_class: Union[Type[Document], Type[DocumentUpdateSchema]]
            Document class used to retrieve unique constraints
        original_document: Optional[FeatureByteBaseDocumentModel]
            Original document before update (provide when checking document update conflict)
        """
        for query_filter, conflict_signature, resolution_signature in self._get_unique_constraints(
            document=document,
            document_class=document_class or self.document_class,
            original_document=original_document,
        ):
            await self._check_document_unique_constraint(
                query_filter=query_filter,
                conflict_signature=conflict_signature,
                resolution_signature=resolution_signature,
            )

    async def update_document(
        self,
        document_id: ObjectId,
        data: DocumentUpdateSchema,
        exclude_none: bool = True,
        document: Optional[Document] = None,
        return_document: bool = True,
    ) -> Optional[Document]:
        """
        Update document at persistent

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        data: DocumentUpdateSchema
            Document update payload object
        exclude_none: bool
            Whether to exclude None value(s) from the data
        document: Optional[Document]
            Document to be updated (when provided, this method won't query persistent for retrieval)
        return_document: bool
            Whether to make additional query to retrieval updated document & return

        Returns
        -------
        Optional[Document]
        """
        if document is None:
            document = await self.get_document(document_id=document_id)

        # perform validation first before actual update
        update_dict = data.dict(exclude_none=exclude_none)
        updated_document = self.document_class(**{**document.json_dict(), **update_dict})

        # check any conflict with existing documents
        await self._check_document_unique_constraints(
            document=updated_document, document_class=type(data), original_document=document
        )

        if document != updated_document:
            # only update if change is detected
            await self.persistent.update_one(
                collection_name=self.collection_name,
                query_filter=self._construct_get_query_filter(document_id=document_id),
                update={"$set": update_dict},
                user_id=self.user.id,
            )

        if return_document:
            return await self.get_document(document_id=document_id)
        return None

    async def historical_document_generator(
        self, document_id: ObjectId
    ) -> AsyncIterator[Optional[Document]]:
        """
        Reconstruct documents of older history

        Parameters
        ----------
        document_id: ObjectId
            Document ID

        Yields
        -------
        AsyncIterator[Optional[Document]]
            Async iterator of older historical records
        """
        async for _, audit_doc in self.persistent.historical_document_generator(
            collection_name=self.collection_name, document_id=document_id
        ):
            if audit_doc:
                yield self.document_class(**audit_doc)
            else:
                yield None
