"""
Persistent storage base class
"""
from typing import Any, Iterable, Mapping, Optional, Union

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
        document: pymongo.typings._DocumentIn
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
        documents: Iterable[pymongo.typings._DocumentIn]
            Documents to insert
        """
        return NotImplemented

    @abstractmethod
    def find_one(self, collection_name: str, filter: Optional[Any]) -> Optional[_DocumentType]:
        """
        Find one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter: Optional[Any]
            Conditions to filter on

        Returns
        -------
        Optional[_DocumentType]
            Retrieved document
        """
        return NotImplemented

    @abstractmethod
    def find(self, collection_name: str, filter: Optional[Any]) -> Iterable[_DocumentType]:
        """
        Find all records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter: Optional[Any]
            Conditions to filter on

        Returns
        -------
        Iterable[_DocumentType]
            Retrieved documents
        """
        return NotImplemented

    @abstractmethod
    def update_one(
        self,
        collection_name: str,
        filter: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
    ) -> int:
        """
        Update one record in collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter: Mapping[str, Any]
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
        filter: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
    ) -> int:
        """
        Update many records in collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter: Mapping[str, Any]
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
    def delete_one(self, collection_name: str, filter: Mapping[str, Any]) -> int:
        """
        Delete one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter: Mapping[str, Any]
            Conditions to filter on

        Returns
        -------
        int
            Number of records deleted
        """
        return NotImplemented

    @abstractmethod
    def delete_many(self, collection_name: str, filter: Mapping[str, Any]) -> int:
        """
        Delete many records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter: Mapping[str, Any]
            Conditions to filter on

        Returns
        -------
        int
            Number of records deleted
        """
        return NotImplemented
