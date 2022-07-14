"""
Persistent storage using MongoDB
"""
from __future__ import annotations

from typing import Any, Iterable, Iterator, List, Literal, Optional, Tuple

from contextlib import contextmanager

import pymongo
from bson.objectid import ObjectId

from featurebyte.persistent.base import (
    Document,
    DocumentUpdate,
    DuplicateDocumentError,
    Persistent,
    QueryFilter,
)


class MongoDB(Persistent):
    """
    Persistent storage using MongoDB
    """

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
        self._database = database
        self._client: Any = pymongo.MongoClient(uri)
        self._db: Any = self._client[self._database]
        self._session = None

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
            result = self._db[collection_name].insert_one(document, session=self._session)
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
            result = self._db[collection_name].insert_many(documents, session=self._session)
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
        return (
            self._db[collection_name]
            .update_one(query_filter, update, session=self._session)
            .modified_count
        )

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
        return (
            self._db[collection_name]
            .update_many(query_filter, update, session=self._session)
            .modified_count
        )

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
        return (
            self._db[collection_name].delete_one(query_filter, session=self._session).deleted_count
        )

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
        return (
            self._db[collection_name].delete_many(query_filter, session=self._session).deleted_count
        )

    @contextmanager
    def start_transaction(self) -> Iterator[MongoDB]:
        """
        MongoDB transaction session context manager

        Yields
        ------
        Iterator[MongoDB]
            MongoDB object
        """
        with self._client.start_session() as session:
            with session.start_transaction():
                self._session = session
                yield self
                self._session = None
