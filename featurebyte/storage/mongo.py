"""
Persistent storage using MongoDB
"""
from typing import Any, Iterable, Mapping, Optional, Union

import pymongo
from pymongo.typings import _DocumentIn, _DocumentType, _Pipeline

from .storage import Storage


class MongoStorage(Storage):
    """
    Persistent storage using MongoDB
    """

    _client: pymongo.MongoClient = None
    _db: pymongo.database.Database = None

    def __init__(self, uri: str, database: str = "featurebyte") -> None:
        """
        Constructor for MongoStorage

        Parameters
        ----------
        uri: str
            MongoDB connection string (e.g. "mongodb://user:pass@localhost:27017")
        database: str
            Database to use
        """
        self._client = pymongo.MongoClient(uri)
        self._db = self._client[database]

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
        self._db[collection_name].insert_one(document)

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
        self._db[collection_name].insert_many(documents)

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
        return self._db[collection_name].find_one(filter)

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
        return self._db[collection_name].find(filter)

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
        return self._db[collection_name].update_one(filter, update).modified_count

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
        return self._db[collection_name].update_many(filter, update).modified_count

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
        return self._db[collection_name].delete_one(filter).deleted_count

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
        return self._db[collection_name].delete_many(filter).deleted_count
