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

    @abstractmethod
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

        Raises
        ------
        DuplicateDocumentError
            Document already exist
        """

    @abstractmethod
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

        Raises
        ------
        DuplicateDocumentError
            Document already exist
        """

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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
        """

    @abstractmethod
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
        """

    @abstractmethod
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

    @abstractmethod
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
