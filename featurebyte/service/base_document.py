"""
BaseService class
"""

from __future__ import annotations

import copy
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Generic, Iterator, List, Optional, Type, TypeVar, Union

import numpy as np
import pandas as pd
from bson import ObjectId
from cachetools import LRUCache
from pymongo.errors import OperationFailure
from redis import Redis
from tenacity import retry, retry_if_exception_type, wait_chain, wait_random

from featurebyte.common.dict_util import get_field_path_value
from featurebyte.common.string import sanitize_search_term
from featurebyte.exception import (
    CatalogNotSpecifiedError,
    DocumentConflictError,
    DocumentModificationBlockedError,
    DocumentNotFoundError,
    QueryNotSupportedError,
)
from featurebyte.logging import get_logger
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteCatalogBaseDocumentModel,
    ReferenceInfo,
    UniqueConstraintResolutionSignature,
    VersionIdentifier,
)
from featurebyte.models.persistent import (
    AuditActionType,
    DocumentUpdate,
    FieldValueHistory,
    QueryFilter,
)
from featurebyte.persistent.base import Persistent, SortDir
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.common.base import (
    BaseDocumentServiceUpdateSchema,
    BaseInfo,
    DocumentSoftDeleteUpdate,
)
from featurebyte.service.mixin import (
    DEFAULT_PAGE_SIZE,
    Document,
    DocumentCreateSchema,
    OpsServiceMixin,
)
from featurebyte.storage import Storage

DocumentUpdateSchema = TypeVar("DocumentUpdateSchema", bound=BaseDocumentServiceUpdateSchema)
InfoDocument = TypeVar("InfoDocument", bound=BaseInfo)
RAW_QUERY_FILTER_WARNING = (
    "Using raw query filter breaks application logic. "
    "It should only be used when absolutely necessary."
)
RETRY_MAX_WAIT_IN_SEC = 2
RETRY_MAX_ATTEMPT_NUM = 3


logger = get_logger(__name__)


@dataclass
class UniqueConstraintData:
    """
    Unique constraint data
    """

    query_filter: QueryFilter
    projection: Dict[str, Any]
    conflict_signature: Dict[str, Any]
    resolution_signature: Optional[UniqueConstraintResolutionSignature]


def as_object_id(object_id: Any) -> ObjectId:
    """
    Convert object_id to ObjectId type if necessary

    Parameters
    ----------
    object_id: Any
        The ObjectId like object to convert. Must not be None.

    Returns
    -------
    ObjectId
    """
    if isinstance(object_id, ObjectId):
        return object_id
    assert object_id is not None
    return ObjectId(object_id)


class BaseDocumentService(
    Generic[Document, DocumentCreateSchema, DocumentUpdateSchema], OpsServiceMixin
):
    """
    BaseDocumentService class is responsible to perform CRUD of the underlying persistent
    collection. It will perform model level validation before writing to persistent and after
    reading from the persistent.
    """

    document_class: Type[Document]
    _remote_attribute_cache: Any = LRUCache(maxsize=1024)

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
        storage: Storage,
        redis: Redis[Any],
    ):
        self.user = user
        self.persistent = persistent
        self.catalog_id = catalog_id
        self._allow_to_use_raw_query_filter = False
        self.block_modification_handler = block_modification_handler
        self.storage = storage
        self.redis = redis
        if self.is_catalog_specific and not catalog_id:
            raise CatalogNotSpecifiedError(
                f"No active catalog specified for service: {self.__class__.__name__}"
            )

    @contextmanager
    def allow_use_raw_query_filter(self) -> Iterator[None]:
        """
        Activate use of raw query filter.
        This should be used ONLY when there is need to access all documents regardless of catalog membership.
        Valid use cases are table migration or table restoration.

        Yields
        -------
        None
            Enable use of raw query filter
        """
        try:
            logger.warning(RAW_QUERY_FILTER_WARNING)
            self._allow_to_use_raw_query_filter = True
            yield
        finally:
            self._allow_to_use_raw_query_filter = False

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
    def is_catalog_specific(self) -> bool:
        """
        Whether service operates on catalog specific documents

        Returns
        -------
        bool
        """
        return issubclass(self.document_class, FeatureByteCatalogBaseDocumentModel)

    @property
    def should_disable_audit(self) -> bool:
        """
        Whether service operates on a collection that should not be audited on write operations.
        This is typically the case for internally used collections that are not directly user
        facing.

        Returns
        -------
        bool
        """
        return not self.document_class.Settings.auditable

    def get_full_remote_file_path(self, path: str) -> Path:
        """
        Get full remote file path (add catalog_id prefix if catalog specific)

        Parameters
        ----------
        path: str
            File path

        Returns
        -------
        Path
        """
        if self.is_catalog_specific:
            return Path(f"catalog/{self.catalog_id}/{path}")
        return Path(path)

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
        document_dict = await self._get_document_dict_to_insert(data)
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document_dict,
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )
        assert insert_id == document_dict["_id"]
        return await self.get_document(document_id=insert_id)

    async def create_many(self, data_list: List[DocumentCreateSchema]) -> None:
        """
        Create multiple documents in the persistent

        Parameters
        ----------
        data_list: List[DocumentCreateSchema]
            Document creation payload objects
        """
        if not data_list:
            return

        documents = []
        for data in data_list:
            document_dict = await self._get_document_dict_to_insert(data)
            documents.append(document_dict)

        insert_ids = await self.persistent.insert_many(
            collection_name=self.collection_name,
            documents=documents,
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )
        assert set(insert_ids) == {doc["_id"] for doc in documents}

    async def _get_document_dict_to_insert(self, data: DocumentCreateSchema) -> Dict[str, Any]:
        kwargs = self._extract_additional_creation_kwargs(data)
        if self.is_catalog_specific:
            kwargs = {**kwargs, "catalog_id": self.catalog_id}

        document = self.document_class(
            **{
                **data.model_dump(by_alias=True),
                **kwargs,
                "user_id": self.user.id,
            },
        )

        # check any conflict with existing documents
        await self._check_document_unique_constraints(
            document=document,
            document_class=self.document_class,
        )
        return dict(document.model_dump(by_alias=True))

    async def construct_get_query_filter(
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

        Raises
        ------
        NotImplementedError
            Using raw query filter without activating override
        """
        _ = self, kwargs
        kwargs = {"_id": ObjectId(as_object_id(document_id))}
        if use_raw_query_filter:
            if not self._allow_to_use_raw_query_filter:
                raise NotImplementedError(RAW_QUERY_FILTER_WARNING)
            return kwargs

        # filter out soft deleted documents
        kwargs["is_deleted"] = {"$ne": True}

        # inject catalog_id into filter if document is catalog specific
        if self.is_catalog_specific:
            kwargs = {**kwargs, "catalog_id": self.catalog_id}
        return kwargs

    async def get_document_as_dict(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
        use_raw_query_filter: bool = False,
        projection: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
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
        projection: Optional[Dict[str, Any]]
            Project fields to return from the query
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        Dict[str, Any]

        Raises
        ------
        DocumentNotFoundError
            If the requested document not found
        """
        query_filter = await self.construct_get_query_filter(
            document_id=document_id, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        document_dict = await self.persistent.find_one(
            collection_name=self.collection_name,
            query_filter=query_filter,
            projection=projection,
        )
        if document_dict is None:
            exception_detail = exception_detail or (
                f'{self.class_name} (id: "{document_id}") not found. Please save the {self.class_name} object first.'
            )
            raise DocumentNotFoundError(exception_detail)
        return document_dict

    async def _populate_remote_attributes(self, document: Document) -> Document:
        _ = self
        return document

    async def get_document(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
        use_raw_query_filter: bool = False,
        populate_remote_attributes: bool = True,
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
        populate_remote_attributes: bool
            Populate attributes that are stored remotely (e.g. file paths)
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        Document
        """
        document_dict = await self.get_document_as_dict(
            document_id=document_id,
            exception_detail=exception_detail,
            use_raw_query_filter=use_raw_query_filter,
            **kwargs,
        )
        document = self.document_class(**document_dict)
        if populate_remote_attributes:
            return await self._populate_remote_attributes(document=document)
        return document

    async def delete_document(
        self,
        document_id: ObjectId,
        exception_detail: Optional[str] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> int:
        """
        Delete document dictionary given document id

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        exception_detail: Optional[str]
            Exception detail message
        use_raw_query_filter: bool
            Use only provided query filter
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        int
            number of records deleted
        """
        document_dict = await self.get_document_as_dict(
            document_id=document_id,
            exception_detail=exception_detail,
            use_raw_query_filter=use_raw_query_filter,
            disable_audit=self.should_disable_audit,
            populate_remote_attributes=False,
            **kwargs,
        )

        # check if document is modifiable
        self._check_document_modifiable(document=document_dict)

        query_filter = await self.construct_get_query_filter(
            document_id=document_id, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        num_of_records_deleted = await self.persistent.delete_one(
            collection_name=self.collection_name,
            query_filter=query_filter,
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )

        # remove remote attributes
        await self._delete_remote_attributes_in_storage(document_dict)
        return int(num_of_records_deleted)

    async def delete_many(
        self,
        query_filter: QueryFilter,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> int:
        """
        Delete multiple documents

        Parameters
        ----------
        query_filter: QueryFilter
            Filter to retrieve documents to be deleted
        use_raw_query_filter: bool
            Use only provided query filter
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        int
            number of records deleted
        """
        query_filter = await self.construct_list_query_filter(
            query_filter=query_filter,
            use_raw_query_filter=use_raw_query_filter,
            **kwargs,
        )

        document_dicts = []
        async for document_dict in self.list_documents_as_dict_iterator(query_filter=query_filter):
            # check if document is modifiable
            self._check_document_modifiable(document=document_dict)
            document_dicts.append(document_dict)

        num_of_records_deleted = await self.persistent.delete_many(
            collection_name=self.collection_name,
            query_filter=query_filter,
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )

        for document_dict in document_dicts:
            await self._delete_remote_attributes_in_storage(document_dict)

        return int(num_of_records_deleted)

    async def _delete_remote_attributes_in_storage(self, document_dict: Dict[str, Any]) -> None:
        for remote_path in self.document_class._get_remote_attribute_paths(document_dict):
            await self.storage.try_delete_if_exists(remote_path)

    async def soft_delete_document(self, document_id: ObjectId) -> None:
        """
        Soft delete document at persistent

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        """
        await self.update_document(
            document_id=document_id,
            data=DocumentSoftDeleteUpdate(is_deleted=True),  # type: ignore[arg-type]
            return_document=False,
        )

    async def soft_delete_documents(self, query_filter: QueryFilter) -> int:
        """
        Soft delete multiple documents at persistent

        Parameters
        ----------
        query_filter: QueryFilter
            Query filter to apply on documents

        Returns
        -------
        int
            Number of soft-deleted documents
        """
        return await self.update_documents(
            query_filter=query_filter,
            update={"$set": {"is_deleted": True}},
        )

    async def construct_list_query_filter(
        self,
        query_filter: Optional[QueryFilter] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> QueryFilter:
        """
        Construct query filter used in list route

        Parameters
        ----------
        query_filter: Optional[QueryFilter]
            Query filter to use as starting point
        use_raw_query_filter: bool
            Use only provided query filter
        kwargs: Any
            Keyword arguments passed to the list controller

        Returns
        -------
        QueryFilter

        Raises
        ------
        NotImplementedError
            Using raw query filter without activating override
        """
        _ = self
        output: QueryFilter
        if not query_filter:
            output = {}
        else:
            output = copy.deepcopy(query_filter)

        if use_raw_query_filter:
            if not self._allow_to_use_raw_query_filter:
                raise NotImplementedError(RAW_QUERY_FILTER_WARNING)
            return output

        include_soft_deleted = kwargs.get("include_soft_deleted", False)
        search_term = sanitize_search_term(kwargs.get("search"))
        if not include_soft_deleted:
            output["is_deleted"] = {"$ne": True}  # exclude soft-deleted documents
        if kwargs.get("name"):
            output["name"] = kwargs["name"]
        if kwargs.get("version"):
            output["version"] = kwargs["version"]
        if search_term:
            # sanitize the search term to prevent regex expression attack
            output["$or"] = [
                {"$text": {"$search": search_term}},
                {"name": {"$regex": search_term, "$options": "i"}},
            ]
        # inject catalog_id into filter if document is catalog specific
        if self.is_catalog_specific:
            output["catalog_id"] = self.catalog_id

        return output

    async def list_documents_as_dict(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
        use_raw_query_filter: bool = False,
        projection: Optional[Dict[str, Any]] = None,
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
        sort_by: Optional[list[tuple[str, SortDir]]]
            Keys and directions used to sort the returning documents
        use_raw_query_filter: bool
            Use only provided query filter
        projection: Optional[Dict[str, Any]]
            Project fields to return from the query
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
        sort_by = sort_by or [("created_at", "desc")]
        query_filter = await self.construct_list_query_filter(
            use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        try:
            docs, total = await self.persistent.find(
                collection_name=self.collection_name,
                query_filter=query_filter,
                projection=projection,
                sort_by=sort_by,
                page=page,
                page_size=page_size,
            )
        except NotImplementedError as exc:
            raise QueryNotSupportedError from exc
        return {"page": page, "page_size": page_size, "total": total, "data": list(docs)}

    async def list_documents_as_dict_iterator(
        self,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
        use_raw_query_filter: bool = False,
        projection: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        List documents iterator to retrieve all the results based on given document service & query filter

        Parameters
        ----------
        sort_by: Optional[list[tuple[str, SortDir]]]
            Keys and directions used to sort the returning documents
        use_raw_query_filter: bool
            Use only provided query filter
        projection: Optional[Dict[str, Any]]
            Project fields to return from the query
        kwargs: Any
            Additional keyword arguments

        Yields
        ------
        AsyncIterator[Dict[str, Any]]
            List query output

        Raises
        ------
        QueryNotSupportedError
            If the persistent query is not supported
        """
        sort_by = sort_by or [("created_at", "desc")]
        query_filter = await self.construct_list_query_filter(
            use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        try:
            docs = await self.persistent.get_iterator(
                collection_name=self.collection_name,
                query_filter=query_filter,
                pipeline=kwargs.get("pipeline"),
                projection=projection,
                sort_by=sort_by,
            )
        except NotImplementedError as exc:
            raise QueryNotSupportedError from exc

        async for doc in docs:
            yield doc

    async def list_documents(
        self,
        query_filter: QueryFilter,
        use_raw_query_filter: bool = False,
        populate_remote_attributes: bool = True,
        **kwargs: Any,
    ) -> List[Document]:
        """
        List documents

        Parameters
        ----------
        query_filter: QueryFilter
            Query filter
        use_raw_query_filter: bool
            Use only provided query filter (without any further processing)
        populate_remote_attributes: bool
            Populate attributes that are stored remotely (e.g. file paths)
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        List[Document]
            List of documents fulfilled the filtering condition
        """
        it = self.list_documents_iterator(
            query_filter=query_filter,
            use_raw_query_filter=use_raw_query_filter,
            populate_remote_attributes=populate_remote_attributes,
            **kwargs,
        )
        return [doc async for doc in it]

    async def list_documents_iterator(
        self,
        query_filter: QueryFilter,
        use_raw_query_filter: bool = False,
        populate_remote_attributes: bool = True,
        **kwargs: Any,
    ) -> AsyncIterator[Document]:
        """
        List documents iterator to retrieve all the results based on given document service & query filter

        Parameters
        ----------
        query_filter: QueryFilter
            Query filter
        use_raw_query_filter: bool
            Use only provided query filter (without any further processing)
        populate_remote_attributes: bool
            Populate attributes that are stored remotely (e.g. file paths)
        kwargs: Any
            Additional keyword arguments

        Yields
        -------
        AsyncIterator[Document]
            List query output
        """
        async for doc in self.list_documents_as_dict_iterator(
            query_filter=query_filter,
            use_raw_query_filter=use_raw_query_filter,
            **kwargs,
        ):
            document = self.document_class(**doc)
            if populate_remote_attributes:
                document = await self._populate_remote_attributes(document=document)
            yield document

    async def _construct_list_audit_query_filter(
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
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
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
        sort_by: Optional[list[tuple[str, SortDir]]]
            Keys and directions used to sort the returning documents
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        dict[str, Any]
            List of documents fulfilled the filtering condition
        """
        query_filter = await self._construct_list_audit_query_filter(
            query_filter=query_filter, **kwargs
        )
        docs, total = await self.persistent.get_audit_logs(
            collection_name=self.collection_name,
            document_id=document_id,
            query_filter=query_filter,
            sort_by=sort_by,
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
            sort_by=[("action_at", "asc")],
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
        query_filter: QueryFilter,
        projection: Dict[str, Any],
        conflict_signature: dict[str, Any],
        resolution_signature: UniqueConstraintResolutionSignature | None,
    ) -> None:
        """
        Check document creation conflict

        Parameters
        ----------
        query_filter: QueryFilter
            Query filter that will be passed to persistent
        projection: Dict[str, Any]
            Projection fields to return from the query
        conflict_signature: dict[str, Any]
            Document representation that shows user the conflict fields
        resolution_signature: UniqueConstraintResolutionSignature | None
            Object retrieval option shows in error message.

        Raises
        ------
        DocumentConflictError
            When there is a conflict with existing document(s) stored at the persistent
        """
        # for catalog specific document type constraint uniqueness checking to the catalog
        if self.is_catalog_specific:
            query_filter = {**query_filter, "catalog_id": self.catalog_id}

        conflict_doc = await self.persistent.find_one(
            collection_name=self.collection_name,
            query_filter=query_filter,
            projection=projection,
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
    ) -> Iterator[UniqueConstraintData]:
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
        Iterator[UniqueConstraintData]
            List of unique constraint data
        """
        doc_dict = document.model_dump(by_alias=True)
        original_dict = original_document.model_dump(by_alias=True) if original_document else None
        for constraint in document_class.Settings.unique_constraints:
            query_filter = {field: doc_dict[field] for field in constraint.fields}

            # default projection will only include _id, name, type (if exists)
            projection = {"_id": 1, "name": 1, "type": 1}
            if original_dict:
                # skip checking if the original document value are the same with the updated one
                original_value = {field: original_dict[field] for field in constraint.fields}
                if original_value == query_filter:
                    continue

                # update projection to include query filter fields
                projection.update({field: 1 for field in constraint.fields})

            if constraint.extra_query_params is not None:
                query_filter.update(constraint.extra_query_params)
                projection.update({field: 1 for field in constraint.extra_query_params})

            conflict_signature = {
                name: get_field_path_value(doc_dict, fields)
                for name, fields in constraint.conflict_fields_signature.items()
            }
            if conflict_signature.get("version"):
                conflict_signature["version"] = VersionIdentifier(
                    **conflict_signature["version"]
                ).to_str()

            yield UniqueConstraintData(
                query_filter=query_filter,
                projection=projection,
                conflict_signature=conflict_signature,
                resolution_signature=constraint.resolution_signature,
            )

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
        for unique_constraint in self._get_unique_constraints(
            document=document,
            document_class=document_class or self.document_class,
            original_document=original_document,
        ):
            await self._check_document_unique_constraint(
                query_filter=unique_constraint.query_filter,
                projection=unique_constraint.projection,
                conflict_signature=unique_constraint.conflict_signature,
                resolution_signature=unique_constraint.resolution_signature,
            )

    def _check_document_modifiable(self, document: Dict[str, Any]) -> None:
        if self.block_modification_handler.block_modification and document.get(
            "block_modification_by"
        ):
            block_modification_by = [
                f"{item['asset_name']}(id: {item['document_id']})"
                for item in document.get("block_modification_by", [])
            ]
            raise DocumentModificationBlockedError(
                f"Document {document['_id']} is blocked from modification by {block_modification_by}"
            )

    async def _update_document(
        self,
        document: Document,
        update_dict: Dict[str, Any],
        update_document_class: Optional[Type[DocumentUpdateSchema]],
        skip_block_modification_check: bool = False,
    ) -> None:
        """
        Update document to persistent

        Parameters
        ----------
        document: Document
            Document
        update_dict: Dict[str, Any]
            Update dictionary
        update_document_class: Optional[DocumentUpdateSchema]
            Document update schema class
        skip_block_modification_check: bool
            Whether to skip block modification check (use with caution,
            should only be used when updating document description)
        """
        if not skip_block_modification_check:
            # check if document is modifiable
            self._check_document_modifiable(document=document.model_dump(by_alias=True))

        # check any conflict with existing documents
        updated_document = self.document_class(**{
            **document.model_dump(by_alias=True),
            **copy.deepcopy(update_dict),
        })
        await self._check_document_unique_constraints(
            document=updated_document,
            document_class=update_document_class,
            original_document=document,
        )

        if document != updated_document:
            # only update if change is detected
            await self.persistent.update_one(
                collection_name=self.collection_name,
                query_filter=await self.construct_get_query_filter(document_id=document.id),
                update={"$set": update_dict},
                user_id=self.user.id,
                disable_audit=self.should_disable_audit,
            )

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
        """
        Update document at persistent

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        data: DocumentUpdateSchema
            Document update payload object
        exclude_none: bool
            Whether to exclude None value(s) from the table
        document: Optional[Document]
            Document to be updated (when provided, this method won't query persistent for retrieval)
        return_document: bool
            Whether to make additional query to retrieval updated document & return
        skip_block_modification_check: bool
            Whether to skip block modification check (use with caution, only use when updating document description)
        populate_remote_attributes: bool
            Whether to populate remote attributes (e.g. file paths) when returning document

        Returns
        -------
        Optional[Document]
        """
        if document is None:
            document = await self.get_document(
                document_id=document_id, populate_remote_attributes=False
            )

        # perform validation first before actual update
        update_dict = data.model_dump(
            exclude_none=exclude_none,
            exclude={"id": True},  # exclude id to avoid updating original document ID
            by_alias=True,  # use alias when getting update data dictionary
        )

        # update document to persistent
        await self._update_document(
            document=document,
            update_dict=update_dict,
            update_document_class=type(data),
            skip_block_modification_check=skip_block_modification_check,
        )

        if return_document:
            return await self.get_document(
                document_id=document_id, populate_remote_attributes=populate_remote_attributes
            )
        return None

    @retry(
        retry=retry_if_exception_type(OperationFailure),
        wait=wait_chain(*[
            wait_random(max=RETRY_MAX_WAIT_IN_SEC) for _ in range(RETRY_MAX_ATTEMPT_NUM)
        ]),
    )
    async def update_documents(
        self,
        query_filter: QueryFilter,
        update: DocumentUpdate,
    ) -> int:
        """
        Update documents at persistent

        Parameters
        ----------
        query_filter: QueryFilter
            Query filter
        update: DocumentUpdate
            Document update payload object

        Returns
        -------
        int
            Number of documents updated
        """
        updated_count = await self.persistent.update_many(
            collection_name=self.collection_name,
            query_filter=query_filter,
            update=update,
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )
        return int(updated_count)

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

    async def add_block_modification_by(
        self, query_filter: QueryFilter, reference_info: ReferenceInfo
    ) -> None:
        """
        Add block modification by to records matching query filter

        Parameters
        ----------
        query_filter: QueryFilter
            Query filter to find records to be blocked
        reference_info: ReferenceInfo
            Reference info of the blocking document
        """
        await self.persistent.update_many(
            collection_name=self.collection_name,
            query_filter=query_filter,
            update={
                "$addToSet": {"block_modification_by": reference_info.model_dump(by_alias=True)},
            },
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )

    async def remove_block_modification_by(
        self, query_filter: QueryFilter, reference_info: ReferenceInfo
    ) -> None:
        """
        Remove block modification by from records matching query filter

        Parameters
        ----------
        query_filter: QueryFilter
            Query filter to find records to be unblocked
        reference_info: ReferenceInfo
            Reference info of the blocking document
        """
        await self.persistent.update_many(
            collection_name=self.collection_name,
            query_filter=query_filter,
            update={
                "$pull": {"block_modification_by": reference_info.model_dump(by_alias=True)},
            },
            user_id=self.user.id,
            disable_audit=self.should_disable_audit,
        )

    async def update_document_description(
        self,
        document_id: ObjectId,
        description: Optional[str],
    ) -> None:
        """
        Update document at persistent

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        description: Optional[str]
            Document description
        """
        document = await self.get_document(document_id=document_id)
        await self._update_document(
            document=document,
            update_dict={"description": description},
            update_document_class=None,
            skip_block_modification_check=True,
        )
