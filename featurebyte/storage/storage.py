"""
Persistent storage base class
"""
from typing import Any, Iterable, Literal, Mapping, Optional, Union

from abc import ABC, abstractmethod

from pymongo.typings import _DocumentIn, _DocumentType, _Pipeline


class Storage(ABC):
    """
    Persistent storage base class
    """

    @abstractmethod
    def insert_one(self, collection_name: str, document: _DocumentIn) -> None:
        """
        Insert record into collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        document: _DocumentIn
            Document to insert
        """
        return NotImplemented

    @abstractmethod
    def insert_many(self, collection_name: str, documents: Iterable[_DocumentIn]) -> None:
        """
        Insert records into collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        documents: Iterable[_DocumentIn]
            Documents to insert
        """
        return NotImplemented

    @abstractmethod
    def find_one(
        self, collection_name: str, filter_query: Optional[Any]
    ) -> Optional[_DocumentType]:
        """
        Find one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: Optional[Any]
            Conditions to filter on

        Returns
        -------
        Optional[_DocumentType]
            Retrieved document
        """
        return NotImplemented

    @abstractmethod
    def find(
        self,
        collection_name: str,
        filter_query: Optional[Any],
        sort_by: Optional[str] = None,
        sort_dir: Optional[Literal["asc", "desc"]] = "asc",
        page: Optional[int] = 1,
        page_size: Optional[int] = 0,
    ) -> Iterable[_DocumentType]:
        """
        Find all records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: Optional[Any]
            Conditions to filter on
        sort_by: Optional[str]
            Column to sort by
        sort_dir: Optional[Literal["asc", "desc"]]
            Direction to sort
        page: Optional[int]
            Page number for pagination
        page_size: Optional[int]
            Page size (0 to return all records)

        Returns
        -------
        Tuple[Iterable[_DocumentType], int]
            Retrieved documents and total count
        """
        return NotImplemented

    @abstractmethod
    def update_one(
        self,
        collection_name: str,
        filter_query: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
    ) -> int:
        """
        Update one record in collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: Mapping[str, Any]
            Conditions to filter on
        update: Union[Mapping[str, Any], _Pipeline]
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
        filter_query: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
    ) -> int:
        """
        Update many records in collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: Mapping[str, Any]
            Conditions to filter on
        update: Union[Mapping[str, Any], _Pipeline]
            Values to update

        Returns
        -------
        int
            Number of records modified
        """
        return NotImplemented

    @abstractmethod
    def delete_one(self, collection_name: str, filter_query: Mapping[str, Any]) -> int:
        """
        Delete one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: Mapping[str, Any]
            Conditions to filter on

        Returns
        -------
        int
            Number of records deleted
        """
        return NotImplemented

    @abstractmethod
    def delete_many(self, collection_name: str, filter_query: Mapping[str, Any]) -> int:
        """
        Delete many records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: Mapping[str, Any]
            Conditions to filter on

        Returns
        -------
        int
            Number of records deleted
        """
        return NotImplemented
