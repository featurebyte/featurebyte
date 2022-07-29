"""
Persistent persistent base class
"""
from __future__ import annotations

from typing import (
    Any,
    AsyncIterator,
    Iterable,
    List,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
)

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager

from bson.objectid import ObjectId

from featurebyte.routes.common.util import get_utc_now

Document = MutableMapping[str, Any]
QueryFilter = MutableMapping[str, Any]
DocumentUpdate = Mapping[str, Any]


class DuplicateDocumentError(Exception):
    """
    Duplicate document found during insert / update
    """


class Persistent(ABC):
    """
    Persistent persistent base class
    """

    async def insert_one(self, collection_name: str, document: Document) -> ObjectId:
        """
        Insert record into collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        document: Document
            Document to insert

        Returns
        -------
        ObjectId
            Id of the inserted document
        """
        document["created_at"] = get_utc_now()
        return await self._insert_one(collection_name=collection_name, document=document)

    async def insert_many(
        self, collection_name: str, documents: Iterable[Document]
    ) -> List[ObjectId]:
        """
        Insert records into collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        documents: Iterable[Document]
            Documents to insert

        Returns
        -------
        List[ObjectId]
            Ids of the inserted document
        """
        utc_now = get_utc_now()
        for document in documents:
            document["created_at"] = utc_now
        return await self._insert_many(collection_name=collection_name, documents=documents)

    async def find_one(self, collection_name: str, query_filter: QueryFilter) -> Optional[Document]:
        """
        Find one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on

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

    async def update_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        update: Document,
    ) -> int:
        """
        Update one record in collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        update: Document
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
        update["$set"] = set_val
        return await self._update_one(
            collection_name=collection_name,
            query_filter=query_filter,
            update=update,
        )

    async def update_many(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        update: Document,
    ) -> int:
        """
        Update many records in collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        update: Document
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
        update["$set"] = set_val
        return await self._update_many(
            collection_name=collection_name,
            query_filter=query_filter,
            update=update,
        )

    async def replace_one(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        replacement: Document,
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

    async def delete_one(self, collection_name: str, query_filter: QueryFilter) -> int:
        """
        Delete one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on

        Returns
        -------
        int
            Number of records deleted
        """
        return await self._delete_one(collection_name=collection_name, query_filter=query_filter)

    async def delete_many(self, collection_name: str, query_filter: QueryFilter) -> int:
        """
        Delete many records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on

        Returns
        -------
        int
            Number of records deleted
        """
        return await self._delete_many(collection_name=collection_name, query_filter=query_filter)

    @abstractmethod
    @asynccontextmanager
    async def start_transaction(self) -> AsyncIterator[Persistent]:
        """
        Context manager for transaction session

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
        update: Document,
    ) -> int:
        pass

    @abstractmethod
    async def _update_many(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        update: Document,
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
