"""
Persistent storage using MongoDB
"""
from __future__ import annotations

from typing import Any, Iterable, List, Literal, Optional, Tuple

import pymongo
from bson.objectid import ObjectId

from featurebyte.persistent.base import (
    Document,
    DocumentUpdate,
    DuplicateDocumentError,
    Persistent,
    QueryFilter,
)


def _populate_document_id(document: Document) -> Document:
    """
    Ensure _id and id are synced

    Parameters
    ----------
    document: Document
        Document object to update

    Returns
    -------
    Document
        Updated document
    """
    if "id" in document:
        document["_id"] = document["id"]
    elif "_id" in document:
        document["id"] = document["_id"]
    else:
        document["_id"] = document["id"] = ObjectId()
    return document


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

    def insert_one(self, collection_name: str, document: Document) -> ObjectId:
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
        try:
            result = self._db[collection_name].insert_one(_populate_document_id(document))
            return ObjectId(result.inserted_id)
        except pymongo.errors.DuplicateKeyError as exc:
            raise DuplicateDocumentError() from exc

    def insert_many(self, collection_name: str, documents: Iterable[Document]) -> List[ObjectId]:
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
        try:
            result = self._db[collection_name].insert_many(
                [_populate_document_id(document) for document in documents]
            )
            return result.inserted_ids
        except pymongo.errors.DuplicateKeyError as exc:
            raise DuplicateDocumentError() from exc

    def find_one(self, collection_name: str, query_filter: QueryFilter) -> Optional[Document]:
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
        Optional[DocumentType]
            Retrieved document
        """
        return self._db[collection_name].find_one(query_filter)

    def find(
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
        cursor = self._db[collection_name].find(query_filter)
        total = self._db[collection_name].count_documents(query_filter)

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
        query_filter: QueryFilter,
        update: DocumentUpdate,
    ) -> int:
        """
        Update one record in collection

        Parameters
        ----------
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
        """
        return self._db[collection_name].update_one(query_filter, update).modified_count

    def update_many(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        update: DocumentUpdate,
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

        Returns
        -------
        int
            Number of records modified
        """
        return self._db[collection_name].update_many(query_filter, update).modified_count

    def delete_one(self, collection_name: str, query_filter: QueryFilter) -> int:
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
        return self._db[collection_name].delete_one(query_filter).deleted_count

    def delete_many(self, collection_name: str, query_filter: QueryFilter) -> int:
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
        return self._db[collection_name].delete_many(query_filter).deleted_count
