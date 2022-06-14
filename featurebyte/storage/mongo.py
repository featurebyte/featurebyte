"""
Persistent storage using MongoDB
"""
from typing import Any, Iterable, Literal, Mapping, Optional, Union

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
        document: _DocumentIn
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
        documents: Iterable[_DocumentIn]
            Documents to insert
        """
        self._db[collection_name].insert_many(documents)

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
        return self._db[collection_name].find_one(filter_query)

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
        Iterable[_DocumentType]
            Retrieved documents
        """
        cursor = self._db[collection_name].find(filter_query)
        total = self._db[collection_name].count_documents(filter_query)

        if sort_by:
            cursor = cursor.sort(
                [(str(sort_by), pymongo.ASCENDING if sort_dir == "asc" else pymongo.DESCENDING)]
            )

        if page_size > 0:
            skips = page_size * (page - 1)
            cursor = cursor.skip(skips).limit(page_size)

        return cursor, total

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
        return self._db[collection_name].update_one(filter_query, update).modified_count

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
        return self._db[collection_name].update_many(filter_query, update).modified_count

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
        return self._db[collection_name].delete_one(filter_query).deleted_count

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
        return self._db[collection_name].delete_many(filter_query).deleted_count
