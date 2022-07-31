"""
Persistent persistent base class
"""
from __future__ import annotations

from typing import AsyncIterator, Iterable, List, Literal, Optional, Tuple

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager

from bson.objectid import ObjectId

from featurebyte.models.persistent import (
    AuditActionType,
    AuditDocument,
    Document,
    DocumentUpdate,
    QueryFilter,
)
from featurebyte.persistent.audit import get_doc_name, get_previous_values
from featurebyte.routes.common.util import get_utc_now


class DuplicateDocumentError(Exception):
    """
    Duplicate document found during insert / update
    """


class Persistent(ABC):
    """
    Persistent persistent base class
    """

    def __init__(self) -> None:
        self._in_transaction: bool = False

    async def insert_one(
        self, collection_name: str, document: Document, user_id: Optional[ObjectId] = None
    ) -> ObjectId:
        """
        Insert record into collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        document: Document
            Document to insert
        user_id: Optional[ObjectId]
            ID of user who performed this operation

        Returns
        -------
        ObjectId
            Id of the inserted document
        """
        async with self.start_transaction() as self:
            document["created_at"] = get_utc_now()
            inserted_id = await self._insert_one(collection_name=collection_name, document=document)

            # create audit doc to track changes
            if inserted_id:
                audit_doc = AuditDocument(
                    user_id=user_id,
                    name=f"insert: {get_doc_name(document)}",
                    document_id=document["_id"],
                    action_type=AuditActionType.INSERT,
                    previous_values={},
                ).dict(by_alias=True)
                await self._insert_one(
                    collection_name=f"__audit__{collection_name}", document=audit_doc
                )

            return inserted_id

    async def insert_many(
        self,
        collection_name: str,
        documents: Iterable[Document],
        user_id: Optional[ObjectId] = None,
    ) -> List[ObjectId]:
        """
        Insert records into collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        documents: Iterable[Document]
            Documents to insert
        user_id: Optional[ObjectId]
            ID of user who performed this operation

        Returns
        -------
        List[ObjectId]
            Ids of the inserted document
        """
        async with self.start_transaction() as self:
            utc_now = get_utc_now()

            # create audit docs to track changes
            audit_docs = []
            for document in documents:
                document["created_at"] = utc_now
                audit_docs.append(
                    AuditDocument(
                        user_id=user_id,
                        name=f"insert: {get_doc_name(document)}",
                        document_id=document["_id"],
                        action_type=AuditActionType.INSERT,
                        previous_values={},
                    ).dict(by_alias=True)
                )

            inserted_ids = await self._insert_many(
                collection_name=collection_name, documents=documents
            )
            if inserted_ids:
                await self._insert_many(
                    collection_name=f"__audit__{collection_name}", documents=audit_docs
                )

            return inserted_ids

    async def find_one(
        self, collection_name: str, query_filter: QueryFilter, user_id: Optional[ObjectId] = None
    ) -> Optional[Document]:
        """
        Find one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        user_id: Optional[ObjectId]
            ID of user who performed this operation

        Returns
        -------
        Optional[Document]
            Retrieved document
        """
        _ = user_id
        return await self._find_one(collection_name=collection_name, query_filter=query_filter)

    async def find(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        sort_by: Optional[str] = None,
        sort_dir: Optional[Literal["asc", "desc"]] = "asc",
        page: int = 1,
        page_size: int = 0,
        user_id: Optional[ObjectId] = None,
    ) -> Tuple[Iterable[Document], int]:
        """
        Find all records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        sort_by: Optional[str]
            Column to sort by
        sort_dir: Optional[Literal["asc", "desc"]]
            Direction to sort
        page: int
            Page number for pagination
        page_size: int
            Page size (0 to return all records)
        user_id: Optional[ObjectId]
            ID of user who performed this operation

        Returns
        -------
        Tuple[Iterable[Document], int]
            Retrieved documents and total count
        """
        _ = user_id
        return await self._find(
            collection_name=collection_name,
            query_filter=query_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )

    async def update_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        update: DocumentUpdate,
        user_id: Optional[ObjectId] = None,
    ) -> int:
        """
        Update one record in collection

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

        Returns
        -------
        int
            Number of records modified

        Raises
        ------
        NotImplementedError
            Unsupported update value
        """
        async with self.start_transaction() as self:
            set_val = update.get("$set", {})
            if not isinstance(set_val, dict):
                raise NotImplementedError("Unsupported update value")
            set_val["updated_at"] = get_utc_now()
            update = {key: set_val if key == "$set" else value for key, value in update.items()}

            # retrieve original document to track changes
            original_doc = await self._find_one(
                collection_name=collection_name,
                query_filter=query_filter,
            )
            if not original_doc:
                return 0

            num_updated = await self._update_one(
                collection_name=collection_name,
                query_filter=query_filter,
                update=update,
            )

            # create audit docs to track changes
            if num_updated:
                updated_doc = await self._find_one(
                    collection_name=collection_name,
                    query_filter={"_id": original_doc["_id"]},
                )
                assert updated_doc
                audit_doc = AuditDocument(
                    user_id=user_id,
                    name=f"update: {get_doc_name(original_doc)}",
                    document_id=updated_doc["_id"],
                    action_type=AuditActionType.UPDATE,
                    previous_values=get_previous_values(original_doc, updated_doc),
                ).dict(by_alias=True)
                await self._insert_one(
                    collection_name=f"__audit__{collection_name}", document=audit_doc
                )

            return num_updated

    async def update_many(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        update: DocumentUpdate,
        user_id: Optional[ObjectId] = None,
    ) -> int:
        """
        Update many records in collection

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

        Returns
        -------
        int
            Number of records modified

        Raises
        ------
        NotImplementedError
            Unsupported update value
        """
        async with self.start_transaction() as self:
            set_val = update.get("$set", {})
            if not isinstance(set_val, dict):
                raise NotImplementedError("Unsupported update value")
            set_val["updated_at"] = get_utc_now()
            update = {key: set_val if key == "$set" else value for key, value in update.items()}

            # retrieve original documents to track changes
            original_docs, num_original_docs = await self._find(
                collection_name=collection_name,
                query_filter=query_filter,
            )

            num_updated = await self._update_many(
                collection_name=collection_name,
                query_filter=query_filter,
                update=update,
            )

            # create audit doc to track changes
            if num_updated:
                updated_docs, num_updated_docs = await self._find(
                    collection_name=collection_name,
                    query_filter={"_id": {"$in": [doc["_id"] for doc in original_docs]}},
                )
                assert num_original_docs == num_updated_docs
                audit_docs = []
                for original_doc, updated_doc in zip(
                    sorted(original_docs, key=lambda item: str(item["_id"])),
                    sorted(updated_docs, key=lambda item: str(item["_id"])),
                ):
                    assert updated_doc["_id"] == original_doc["_id"]
                    audit_docs.append(
                        AuditDocument(
                            user_id=user_id,
                            name=f"update: {get_doc_name(original_doc)}",
                            document_id=updated_doc["_id"],
                            action_type=AuditActionType.UPDATE,
                            previous_values=get_previous_values(original_doc, updated_doc),
                        ).dict(by_alias=True)
                    )
                await self._insert_many(
                    collection_name=f"__audit__{collection_name}", documents=audit_docs
                )
            return num_updated

    async def replace_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        replacement: Document,
        user_id: Optional[ObjectId] = None,
    ) -> int:
        """
        Replace one record in collection

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

        Returns
        -------
        int
            Number of records modified
        """
        async with self.start_transaction() as self:
            replacement["created_at"] = replacement["updated_at"] = get_utc_now()

            # retrieve original document to track changes
            original_doc = await self._find_one(
                collection_name=collection_name,
                query_filter=query_filter,
            )
            if not original_doc:
                return 0

            num_updated = await self._replace_one(
                collection_name=collection_name,
                query_filter=query_filter,
                replacement=replacement,
            )

            # create audit doc to track changes
            if num_updated:
                updated_doc = await self._find_one(
                    collection_name=collection_name,
                    query_filter={"_id": original_doc["_id"]},
                )
                assert updated_doc
                audit_doc = AuditDocument(
                    user_id=user_id,
                    name=f"replace: {get_doc_name(original_doc)}",
                    document_id=updated_doc["_id"],
                    action_type=AuditActionType.REPLACE,
                    previous_values=get_previous_values(original_doc, updated_doc),
                ).dict(by_alias=True)
                await self._insert_one(
                    collection_name=f"__audit__{collection_name}", document=audit_doc
                )

            return num_updated

    async def delete_one(
        self, collection_name: str, query_filter: QueryFilter, user_id: Optional[ObjectId] = None
    ) -> int:
        """
        Delete one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        user_id: Optional[ObjectId]
            ID of user who performed this operation

        Returns
        -------
        int
            Number of records deleted
        """
        async with self.start_transaction() as self:
            # retrieve original document to track changes
            original_doc = await self._find_one(
                collection_name=collection_name,
                query_filter=query_filter,
            )
            if not original_doc:
                return 0

            num_deleted = await self._delete_one(
                collection_name=collection_name, query_filter=query_filter
            )

            # create audit doc to track changes
            if num_deleted:
                audit_doc = AuditDocument(
                    user_id=user_id,
                    name=f"delete: {get_doc_name(original_doc)}",
                    document_id=original_doc["_id"],
                    action_type=AuditActionType.DELETE,
                    previous_values=original_doc,
                ).dict(by_alias=True)
                await self._insert_one(
                    collection_name=f"__audit__{collection_name}", document=audit_doc
                )

            return num_deleted

    async def delete_many(
        self, collection_name: str, query_filter: QueryFilter, user_id: Optional[ObjectId] = None
    ) -> int:
        """
        Delete many records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        user_id: Optional[ObjectId]
            ID of user who performed this operation

        Returns
        -------
        int
            Number of records deleted
        """
        async with self.start_transaction() as self:
            # retrieve original documents to track changes
            original_docs, _ = await self._find(
                collection_name=collection_name,
                query_filter=query_filter,
            )

            num_deleted = await self._delete_many(
                collection_name=collection_name, query_filter=query_filter
            )

            # create audit doc to track changes
            if num_deleted:
                audit_docs = [
                    AuditDocument(
                        user_id=user_id,
                        name=f"delete: {get_doc_name(original_doc)}",
                        document_id=original_doc["_id"],
                        action_type=AuditActionType.DELETE,
                        previous_values=original_doc,
                    ).dict(by_alias=True)
                    for original_doc in original_docs
                ]
                await self._insert_many(
                    collection_name=f"__audit__{collection_name}", documents=audit_docs
                )

            return num_deleted

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
    async def _insert_one(self, collection_name: str, document: Document) -> ObjectId:
        pass

    @abstractmethod
    async def _insert_many(
        self, collection_name: str, documents: Iterable[Document]
    ) -> List[ObjectId]:
        pass

    @abstractmethod
    async def _find_one(
        self, collection_name: str, query_filter: QueryFilter
    ) -> Optional[Document]:
        pass

    @abstractmethod
    async def _find(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        sort_by: Optional[str] = None,
        sort_dir: Optional[Literal["asc", "desc"]] = "asc",
        page: int = 1,
        page_size: int = 0,
    ) -> Tuple[Iterable[Document], int]:
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
