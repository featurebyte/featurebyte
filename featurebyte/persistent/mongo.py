"""
Persistent storage using MongoDB
"""
from __future__ import annotations

from typing import Any, Iterable, List, Literal, Optional, Tuple

import pymongo
from bson.objectid import ObjectId

from .persistent import DocumentType, DuplicateDocumentError, Persistent


class MongoDB(Persistent):
    """
    Persistent storage using MongoDB
    """

    _client: pymongo.mongo_client.MongoClient[Any]
    _db: pymongo.database.Database[Any]

    def __init__(self, uri: str, database: str = "featurebyte") -> None:
        """
        Constructor for MongoDB

        Parameters
        ----------
        uri: str
            MongoDB connection string (e.g. "mongodb://user:pass@localhost:27017")
        database: str
            Database to use
        """
        self._client = pymongo.MongoClient(uri)  # type: ignore
        self._db = self._client[database]

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
        try:
            result = self._db[collection_name].insert_one(document)
            return ObjectId(result.inserted_id)
        except pymongo.errors.DuplicateKeyError as exc:
            raise DuplicateDocumentError() from exc

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
        try:
            result = self._db[collection_name].insert_many(documents)
            return result.inserted_ids
        except pymongo.errors.DuplicateKeyError as exc:
            raise DuplicateDocumentError() from exc

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
        return self._db[collection_name].find_one(filter_query)

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
        return self._db[collection_name].update_one(filter_query, update).modified_count

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
        return self._db[collection_name].update_many(filter_query, update).modified_count

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
        return self._db[collection_name].delete_one(filter_query).deleted_count

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
        return self._db[collection_name].delete_many(filter_query).deleted_count
