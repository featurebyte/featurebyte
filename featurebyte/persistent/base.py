"""
Persistent base class
"""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)
from uuid import UUID

from bson import ObjectId
from typing_extensions import Literal

from featurebyte.common.model_util import get_utc_now
from featurebyte.models.persistent import (
    AuditActionType,
    AuditDocument,
    AuditTransactionMode,
    Document,
    DocumentUpdate,
    QueryFilter,
)
from featurebyte.persistent.audit import (
    audit_transaction,
    get_audit_collection_name,
    get_previous_and_current_values,
)


class DuplicateDocumentError(Exception):
    """
    Duplicate document found during insert / update
    """


SortDir = Literal["asc", "desc"]


class Persistent(ABC):
    """
    Persistent base class
    """

    def __init__(self) -> None:
        self._in_transaction: bool = False

    @audit_transaction(mode=AuditTransactionMode.SINGLE, action_type=AuditActionType.INSERT)
    async def insert_one(
        self,
        collection_name: str,
        document: Document,
        user_id: Optional[ObjectId],
        disable_audit: bool = False,
    ) -> Union[ObjectId, UUID]:
        """
        Insert record into collection. Note that when using this method inside a non BaseDocumentService,
        please use with caution as it does not inject user_id and catalog_id into the document automatically.

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        document: Document
            Document to insert
        user_id: Optional[ObjectId]
            ID of user who performed this operation
        disable_audit: bool
            Whether to disable creating an audit record for this operation

        Returns
        -------
        Union[ObjectId, UUID]
            Id of the inserted document
        """
        document["created_at"] = get_utc_now()
        return await self._insert_one(collection_name=collection_name, document=document)

    @audit_transaction(mode=AuditTransactionMode.MULTI, action_type=AuditActionType.INSERT)
    async def insert_many(
        self,
        collection_name: str,
        documents: Iterable[Document],
        user_id: Optional[ObjectId],
        disable_audit: bool = False,
    ) -> list[Union[ObjectId, UUID]]:
        """
        Insert records into collection. Note that when using this method inside a non BaseDocumentService,
        please use with caution as it does not inject user_id and catalog_id into the document automatically.

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        documents: Iterable[Document]
            Documents to insert
        user_id: Optional[ObjectId]
            ID of user who performed this operation
        disable_audit: bool
            Whether to disable creating an audit record for this operation

        Returns
        -------
        list[Union[ObjectId, UUID]]
            Ids of the inserted document
        """
        utc_now = get_utc_now()
        for document in documents:
            document["created_at"] = utc_now

        return await self._insert_many(collection_name=collection_name, documents=documents)

    async def find_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        projection: Optional[dict[str, Any]] = None,
    ) -> Optional[Document]:
        """
        Find one record from collection. Note that when using this method inside a non BaseDocumentService,
        please use with caution as it does not inject catalog_id into the query filter automatically.

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        projection: Optional[dict[str, Any]]
            Fields to project

        Returns
        -------
        Optional[Document]
            Retrieved document
        """
        return await self._find_one(
            collection_name=collection_name, query_filter=query_filter, projection=projection
        )

    async def find(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        projection: Optional[dict[str, Any]] = None,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
        page: int = 1,
        page_size: int = 0,
    ) -> tuple[Iterable[Document], int]:
        """
        Find all records from collection. Note that when using this method inside a non BaseDocumentService,
        please use with caution as it does not inject catalog_id into the query filter automatically.

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        projection: Optional[dict[str, Any]]
            Fields to project
        sort_by: Optional[list[tuple[str, SortDir]]]
            Columns and directions to sort by
        page: int
            Page number for pagination
        page_size: int
            Page size (0 to return all records)

        Returns
        -------
        tuple[Iterable[Document], int]
            Retrieved documents and total count
        """
        return await self._find(
            collection_name=collection_name,
            query_filter=query_filter,
            projection=projection,
            sort_by=sort_by,
            page=page,
            page_size=page_size,
        )

    async def get_iterator(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        pipeline: Optional[list[dict[str, Any]]] = None,
        projection: Optional[dict[str, Any]] = None,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
    ) -> AsyncIterator[Document]:
        """
        Find all records from collection. Note that when using this method inside a non BaseDocumentService,
        please use with caution as it does not inject catalog_id into the query filter automatically.

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        pipeline: Optional[list[dict[str, Any]]]
            Pipeline to execute
        projection: Optional[dict[str, Any]]
            Fields to project
        sort_by: Optional[list[tuple[str, SortDir]]]
            Columns and directions to sort by

        Returns
        -------
        AsyncIterator[Dict[str, Any]]
            Retrieved documents
        """
        return await self._get_iterator(
            collection_name=collection_name,
            query_filter=query_filter,
            pipeline=pipeline,
            projection=projection,
            sort_by=sort_by,
        )

    @audit_transaction(mode=AuditTransactionMode.SINGLE, action_type=AuditActionType.UPDATE)
    async def update_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        update: DocumentUpdate,
        user_id: Optional[ObjectId],
        disable_audit: bool = False,
    ) -> int:
        """
        Update one record in collection. Note that when using this method inside a non BaseDocumentService,
        please use with caution as it does not inject catalog_id into the query filter automatically, and
        it does not inject user_id and catalog_id into the update automatically.

        Parameters
        ----------
        user_id: Optional[ObjectId]
            ID of user who performed this operation
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        update: DocumentUpdate
            Values to update
        disable_audit: bool
            Whether to disable creating an audit record for this operation

        Returns
        -------
        int
            Number of records modified

        Raises
        ------
        NotImplementedError
            Unsupported update value
        """
        set_val = update.get("$set", {})
        if not isinstance(set_val, dict):
            raise NotImplementedError("Unsupported update value")
        set_val["updated_at"] = get_utc_now()
        update = {key: set_val if key == "$set" else value for key, value in update.items()}

        return await self._update_one(
            collection_name=collection_name,
            query_filter=query_filter,
            update=update,
        )

    @audit_transaction(mode=AuditTransactionMode.MULTI, action_type=AuditActionType.UPDATE)
    async def update_many(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        update: DocumentUpdate,
        user_id: Optional[ObjectId],
        disable_audit: bool = False,
    ) -> int:
        """
        Update many records in collection. Note that when using this method inside a non BaseDocumentService,
        please use with caution as it does not inject catalog_id into the query filter automatically, and
        it does not inject user_id and catalog_id into the update automatically.

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        update: DocumentUpdate
            Values to update
        user_id: Optional[ObjectId]
            ID of user who performed this operation
        disable_audit: bool
            Whether to disable creating an audit record for this operation

        Returns
        -------
        int
            Number of records modified

        Raises
        ------
        NotImplementedError
            Unsupported update value
        """
        set_val = update.get("$set", {})
        if not isinstance(set_val, dict):
            raise NotImplementedError("Unsupported update value")
        set_val["updated_at"] = get_utc_now()
        update = {key: set_val if key == "$set" else value for key, value in update.items()}

        return await self._update_many(
            collection_name=collection_name,
            query_filter=query_filter,
            update=update,
        )

    @audit_transaction(mode=AuditTransactionMode.SINGLE, action_type=AuditActionType.REPLACE)
    async def replace_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        replacement: Document,
        user_id: Optional[ObjectId],
        disable_audit: bool = False,
    ) -> int:
        """
        Replace one record in collection. Note that when using this method inside a non BaseDocumentService,
        please use with caution as it does not inject catalog_id into the query filter automatically, and
        it does not inject user_id and catalog_id into the update automatically.

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        replacement: Document
            New document to replace existing one
        user_id: Optional[ObjectId]
            ID of user who performed this operation
        disable_audit: bool
            Whether to disable creating an audit record for this operation

        Returns
        -------
        int
            Number of records modified
        """
        replacement["created_at"] = replacement["updated_at"] = get_utc_now()
        return await self._replace_one(
            collection_name=collection_name,
            query_filter=query_filter,
            replacement=replacement,
        )

    @audit_transaction(mode=AuditTransactionMode.SINGLE, action_type=AuditActionType.DELETE)
    async def delete_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        user_id: Optional[ObjectId],
        disable_audit: bool = False,
    ) -> int:
        """
        Delete one record from collection. Note that when using this method inside a non BaseDocumentService,
        please use with caution as it does not inject catalog_id into the query filter automatically.

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        user_id: Optional[ObjectId]
            ID of user who performed this operation
        disable_audit: bool
            Whether to disable creating an audit record for this operation

        Returns
        -------
        int
            Number of records deleted
        """
        return await self._delete_one(collection_name=collection_name, query_filter=query_filter)

    @audit_transaction(mode=AuditTransactionMode.MULTI, action_type=AuditActionType.DELETE)
    async def delete_many(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        user_id: Optional[ObjectId],
        disable_audit: bool = False,
    ) -> int:
        """
        Delete many records from collection. Note that when using this method inside a non BaseDocumentService,
        please use with caution as it does not inject catalog_id into the query filter automatically.

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        user_id: Optional[ObjectId]
            ID of user who performed this operation
        disable_audit: bool
            Whether to disable creating an audit record for this operation

        Returns
        -------
        int
            Number of records deleted
        """
        return await self._delete_many(collection_name=collection_name, query_filter=query_filter)

    async def perma_delete(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        with_audit: bool = True,
    ) -> int:
        """
        Permanently delete records from collection and optionally its audit records

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        with_audit: bool
            Whether to delete audit records as well

        Returns
        -------
        int
            Number of records deleted
        """
        if with_audit:
            doc_iterator = await self.get_iterator(collection_name, query_filter)
            doc_ids = [doc["_id"] async for doc in doc_iterator]
            await self.delete_many(
                collection_name=get_audit_collection_name(collection_name),
                query_filter={"document_id": {"$in": doc_ids}},
                user_id=None,
                disable_audit=True,
            )

        deleted_cnt = await self.delete_many(
            collection_name=collection_name,
            query_filter=query_filter,
            user_id=None,
            disable_audit=True,
        )
        return int(deleted_cnt)

    @asynccontextmanager
    async def start_transaction(self) -> AsyncIterator[Persistent]:
        """
        Context manager for transaction self

        Yields
        ------
        AsyncIterator[Persistent]
            Persistent object
        """
        if self._in_transaction:
            # prevent entering nested transaction in the actual db layer
            yield self
        else:
            try:
                async with self._start_transaction():
                    self._in_transaction = True
                    yield self
            finally:
                self._in_transaction = False

    async def get_audit_logs(
        self,
        collection_name: str,
        document_id: ObjectId,
        query_filter: Optional[QueryFilter] = None,
        projection: Optional[dict[str, Any]] = None,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
        page: int = 1,
        page_size: int = 0,
    ) -> tuple[Iterable[Document], int]:
        """
        Retrieve audit records for a document

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        document_id: ObjectId
            ID of document to use
        query_filter: Optional[QueryFilter]
            Conditions to filter on
        projection: Optional[dict[str, Any]]
            Fields to project
        sort_by: Optional[list[tuple[str, SortDir]]]
            Columns and directions to sort by
        page: int
            Page number for pagination
        page_size: int
            Page size (0 to return all records)

        Returns
        -------
        list[Document]
        """
        sort_by = sort_by or [("_id", "desc")]
        _query_filter = copy.deepcopy(query_filter) if query_filter else {}
        _query_filter["document_id"] = document_id
        return await self._find(
            collection_name=get_audit_collection_name(collection_name),
            query_filter=_query_filter,
            projection=projection,
            sort_by=sort_by,
            page=page,
            page_size=page_size,
        )

    async def historical_document_generator(
        self, collection_name: str, document_id: ObjectId
    ) -> AsyncIterator[tuple[AuditDocument, dict[str, Any]]]:
        """
        Traverse the audit history & reconstructed the document records

        Parameters
        ----------
        collection_name: str
            Collection name (non-audit one)
        document_id: ObjectId
            Document ID

        Yields
        ------
        AuditDocument
            Audit document
        dict[str, Any]
            Document
        """
        docs, _ = await self.get_audit_logs(
            collection_name=collection_name, document_id=document_id, page=1, page_size=0
        )
        sorted_audit_data = sorted(docs, key=lambda record: (record["action_at"], record["_id"]))

        doc = {}
        for audit_doc in sorted_audit_data:
            previous_values = audit_doc["previous_values"]
            current_values = audit_doc["current_values"]
            if audit_doc["action_type"] in {AuditActionType.INSERT, AuditActionType.REPLACE}:
                doc = current_values
                doc["_id"] = audit_doc["document_id"]
            elif audit_doc["action_type"] == AuditActionType.UPDATE:
                doc = {
                    key: current_values.get(key, doc.get(key))
                    for key in set(current_values).union(set(doc).difference(previous_values))
                }
            else:
                doc = {}
            yield AuditDocument(**audit_doc), doc

    async def migrate_record(
        self,
        collection_name: str,
        document: Document,
        migrate_func: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
        skip_audit: bool = False,
    ) -> None:
        """
        Migrate record & its audit records

        Parameters
        ----------
        collection_name: str
            Collection name
        document: Document
            Document to be migrated
        migrate_func: Callable[[dict[str, Any]], dict[str, Any]]
            Function to migrate the record from old to new format
        skip_audit: bool
            Whether to skip migrating audit records
        """
        await self._migrate_record(
            collection_name=collection_name,
            document=document,
            migrate_func=migrate_func,
        )
        if not skip_audit:
            await self._migrate_audit_records(
                collection_name=collection_name,
                document_id=document["_id"],
                migrate_func=migrate_func,
            )

    async def _migrate_record(
        self,
        collection_name: str,
        document: Document,
        migrate_func: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> None:
        """
        Migrate record helper

        Parameters
        ----------
        collection_name: str
            Collection name
        document: Document
            Document to be migrated
        migrate_func: Callable[[dict[str, Any]], dict[str, Any]]
            Function to migrate the record from old to new format
        """
        query_filter = {"_id": document["_id"]}
        await self._replace_one(
            collection_name=collection_name,
            query_filter=query_filter,
            replacement=await migrate_func(cast(Dict[str, Any], document)),
        )

    async def _migrate_audit_records(
        self,
        collection_name: str,
        document_id: ObjectId,
        migrate_func: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> None:
        """
        Migrate audit records helper

        Parameters
        ----------
        collection_name: str
            Collection name (non-audit)
        document_id: ObjectId
            Document ID
        migrate_func: Callable[[dict[str, Any]], dict[str, Any]]
            Function to migrate the record from old to new format
        """
        doc_generator = self.historical_document_generator(
            collection_name=collection_name, document_id=document_id
        )
        previous: dict[str, Any] = {}
        async for audit_doc, doc_dict in doc_generator:
            doc_dict = (await migrate_func(doc_dict)) if doc_dict else {}

            if audit_doc.action_type == AuditActionType.INSERT:
                original_doc = {"_id": doc_dict["_id"]}
            else:
                original_doc = previous

            previous_values, current_values = get_previous_and_current_values(
                original_doc, doc_dict
            )

            updated_audit_doc = AuditDocument(**{
                **audit_doc.model_dump(by_alias=True),
                "previous_values": previous_values,
                "current_values": current_values,
            })
            await self._update_one(
                collection_name=get_audit_collection_name(collection_name),
                query_filter={"_id": audit_doc.id},
                update={
                    "$set": {
                        "previous_values": updated_audit_doc.previous_values,
                        "current_values": updated_audit_doc.current_values,
                    }
                },
            )

            # track previous values
            previous = doc_dict

    @abstractmethod
    @asynccontextmanager
    async def _start_transaction(self) -> AsyncIterator[Persistent]:
        """
        Context manager for transaction self

        Yields
        ------
        AsyncIterator[Persistent]
            Persistent object
        """
        yield self

    @abstractmethod
    async def _insert_one(self, collection_name: str, document: Document) -> Union[ObjectId, UUID]:
        pass

    @abstractmethod
    async def _insert_many(
        self, collection_name: str, documents: Iterable[Document]
    ) -> list[Union[ObjectId, UUID]]:
        pass

    @abstractmethod
    async def _find_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        projection: Optional[dict[str, Any]] = None,
    ) -> Optional[Document]:
        pass

    @abstractmethod
    async def _find(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        projection: Optional[dict[str, Any]] = None,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
        page: int = 1,
        page_size: int = 0,
    ) -> tuple[Iterable[Document], int]:
        pass

    @abstractmethod
    async def _get_iterator(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        pipeline: Optional[list[dict[str, Any]]] = None,
        projection: Optional[dict[str, Any]] = None,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
    ) -> AsyncIterator[Document]:
        pass

    @abstractmethod
    async def _update_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        update: DocumentUpdate,
    ) -> int:
        pass

    @abstractmethod
    async def _update_many(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        update: DocumentUpdate,
    ) -> int:
        pass

    @abstractmethod
    async def _replace_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        replacement: Document,
    ) -> int:
        pass

    @abstractmethod
    async def _delete_one(self, collection_name: str, query_filter: QueryFilter) -> int:
        pass

    @abstractmethod
    async def _delete_many(self, collection_name: str, query_filter: QueryFilter) -> int:
        pass

    @abstractmethod
    async def list_collection_names(self) -> list[str]:
        """
        List collection names

        Returns
        -------
        list[str]
        """

    @abstractmethod
    async def aggregate_find(
        self,
        collection_name: str,
        pipeline: List[Dict[str, Any]],
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
        page: int = 1,
        page_size: int = 0,
        **kwargs: Any,
    ) -> Tuple[Iterable[Document], int]:
        """
        Execute aggregation pipeline

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        pipeline: List[Dict[str, Any]],
            Pipeline to execute
        sort_by: Optional[list[tuple[str, SortDir]]]
            Columns and directions to sort by
        page: int
            Page number for pagination
        page_size: int
            Page size (0 to return all records)
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        Tuple[Iterable[Document], int]
            Retrieved documents and total count
        """

    async def rename_collection(self, collection_name: str, new_collection_name: str) -> None:
        """
        Rename collection

        Parameters
        ----------
        collection_name: str
            From collection name
        new_collection_name: str
            To collection name
        """
        await self._rename_collection(collection_name, new_collection_name)
        await self._rename_collection(
            get_audit_collection_name(collection_name),
            get_audit_collection_name(new_collection_name),
        )

    @abstractmethod
    async def _rename_collection(self, collection_name: str, new_collection_name: str) -> None:
        pass
