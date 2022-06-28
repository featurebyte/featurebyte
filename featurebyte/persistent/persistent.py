"""
Persistent persistent base class
"""
from __future__ import annotations

from typing import Any, Iterable, List, Literal, MutableMapping, Optional, Tuple

from abc import ABC, abstractmethod

from bson.objectid import ObjectId

DocumentType = MutableMapping[str, Any]


class DuplicateDocumentError(Exception):
    """
    Duplicate document found during insert / update
    """


class Persistent(ABC):
    """
    Persistent persistent base class
    """

    @abstractmethod
    def insert_one(self, collection_name: str, document: DocumentType) -> ObjectId:
        """
        Insert record into collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        document: DocumentType
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
    def insert_many(
        self, collection_name: str, documents: Iterable[DocumentType]
    ) -> List[ObjectId]:
        """
        Insert records into collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        documents: Iterable[DocumentType]
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
    def find_one(self, collection_name: str, filter_query: DocumentType) -> Optional[DocumentType]:
        """
        Find one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
            Conditions to filter on

        Returns
        -------
        Optional[DocumentType]
            Retrieved document
        """
        return NotImplemented

    @abstractmethod
    def find(
        self,
        collection_name: str,
        filter_query: DocumentType,
        sort_by: Optional[str] = None,
        sort_dir: Optional[Literal["asc", "desc"]] = "asc",
        page: int = 1,
        page_size: int = 0,
    ) -> Tuple[Iterable[DocumentType], int]:
        """
        Find all records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
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
        Tuple[Iterable[DocumentType], int]
            Retrieved documents and total count
        """
        return NotImplemented

    @abstractmethod
    def update_one(
        self,
        collection_name: str,
        filter_query: DocumentType,
        update: DocumentType,
    ) -> int:
        """
        Update one record in collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
            Conditions to filter on
        update: DocumentType
            Values to update

        Returns
        -------
        int
            Number of records modified
        """
        return NotImplemented

    @abstractmethod
    def update_many(
        self,
        collection_name: str,
        filter_query: DocumentType,
        update: DocumentType,
    ) -> int:
        """
        Update many records in collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
            Conditions to filter on
        update: DocumentType
            Values to update

        Returns
        -------
        int
            Number of records modified
        """
        return NotImplemented

    @abstractmethod
    def delete_one(self, collection_name: str, filter_query: DocumentType) -> int:
        """
        Delete one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
            Conditions to filter on

        Returns
        -------
        int
            Number of records deleted
        """
        return NotImplemented

    @abstractmethod
    def delete_many(self, collection_name: str, filter_query: DocumentType) -> int:
        """
        Delete many records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
            Conditions to filter on

        Returns
        -------
        int
            Number of records deleted
        """
        return NotImplemented
