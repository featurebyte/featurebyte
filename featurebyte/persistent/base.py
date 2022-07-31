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
    AuditTransactionMode,
    Document,
    DocumentUpdate,
    QueryFilter,
)
from featurebyte.persistent.audit import audit_transaction
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

    @audit_transaction(mode=AuditTransactionMode.SINGLE, action_type=AuditActionType.INSERT)
    async def insert_one(
        self,
        collection_name: str,
        document: Document,
        user_id: Optional[ObjectId] = None,  # pylint: disable=unused-argument
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
        document["created_at"] = get_utc_now()
        return await self._insert_one(collection_name=collection_name, document=document)

    @audit_transaction(mode=AuditTransactionMode.MULTI, action_type=AuditActionType.INSERT)
    async def insert_many(
        self,
        collection_name: str,
        documents: Iterable[Document],
        user_id: Optional[ObjectId] = None,  # pylint: disable=unused-argument
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
        utc_now = get_utc_now()
        for document in documents:
            document["created_at"] = utc_now

        return await self._insert_many(collection_name=collection_name, documents=documents)

    async def find_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        user_id: Optional[ObjectId] = None,  # pylint: disable=unused-argument
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
        return await self._find_one(collection_name=collection_name, query_filter=query_filter)

    async def find(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        sort_by: Optional[str] = None,
        sort_dir: Optional[Literal["asc", "desc"]] = "asc",
        page: int = 1,
        page_size: int = 0,
        user_id: Optional[ObjectId] = None,  # pylint: disable=unused-argument
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
        return await self._find(
            collection_name=collection_name,
            query_filter=query_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )

    @audit_transaction(mode=AuditTransactionMode.SINGLE, action_type=AuditActionType.UPDATE)
    async def update_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        update: DocumentUpdate,
        user_id: Optional[ObjectId] = None,  # pylint: disable=unused-argument
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
        user_id: Optional[ObjectId] = None,  # pylint: disable=unused-argument
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
        user_id: Optional[ObjectId] = None,  # pylint: disable=unused-argument
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
        user_id: Optional[ObjectId] = None,  # pylint: disable=unused-argument
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
        return await self._delete_one(collection_name=collection_name, query_filter=query_filter)

    @audit_transaction(mode=AuditTransactionMode.MULTI, action_type=AuditActionType.DELETE)
    async def delete_many(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        user_id: Optional[ObjectId] = None,  # pylint: disable=unused-argument
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
        return await self._delete_many(collection_name=collection_name, query_filter=query_filter)

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
