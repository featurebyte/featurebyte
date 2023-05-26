"""
Persistent storage using MongoDB
"""
from __future__ import annotations

from typing import Any, AsyncIterator, Iterable, List, Literal, Optional, Tuple, cast

import asyncio
from contextlib import asynccontextmanager

import pymongo
from bson.objectid import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.results import DeleteResult, InsertManyResult, InsertOneResult, UpdateResult

from featurebyte.models.persistent import Document, DocumentUpdate, QueryFilter
from featurebyte.persistent.base import DuplicateDocumentError, Persistent


class MongoDB(Persistent):
    """
    Persistent storage using MongoDB. Note that this class is not thread-safe.
    Do not use the same instance in multiple threads.
    """

    def __init__(self, uri: str, database: str = "featurebyte", client: Any = None) -> None:
        """
        Constructor for MongoDB

        Parameters
        ----------
        uri: str
            MongoDB connection string (e.g. "mongodb://user:pass@localhost:27017")
        database: str
            Database to use
        client: Any
            Client to use
        """
        super().__init__()
        self._database = database
        self._client = client or AsyncIOMotorClient(uri, uuidRepresentation="standard")
        self._db = self._client[self._database]
        self._session: Any = None
        # ensure client uses current loop
        if hasattr(self._client, "get_io_loop"):
            self._client.get_io_loop = asyncio.get_running_loop

    async def _insert_one(self, collection_name: str, document: Document) -> ObjectId:
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
            result: InsertOneResult = await self._db[collection_name].insert_one(
                document, session=self._session
            )
            return ObjectId(result.inserted_id)
        except pymongo.errors.DuplicateKeyError as exc:
            raise DuplicateDocumentError() from exc

    async def _insert_many(
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
        try:
            result: InsertManyResult = await self._db[collection_name].insert_many(
                documents, session=self._session
            )
            return result.inserted_ids
        except pymongo.errors.DuplicateKeyError as exc:
            raise DuplicateDocumentError() from exc

    async def _find_one(
        self, collection_name: str, query_filter: QueryFilter
    ) -> Optional[Document]:
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
        result: Optional[Document] = await self._db[collection_name].find_one(
            query_filter, session=self._session
        )
        return result

    async def _find(
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
        cursor = self._db[collection_name].find(query_filter, session=self._session)
        total = await self._db[collection_name].count_documents(query_filter, session=self._session)

        if sort_by:
            cursor = cursor.sort(
                [
                    (str(sort_by), pymongo.ASCENDING if sort_dir == "asc" else pymongo.DESCENDING),
                    ("_id", pymongo.DESCENDING),  # break ties using _id
                ]
            )

        if page_size > 0:
            skips = page_size * (page - 1)
            cursor = cursor.skip(skips).limit(page_size)

        result: List[Document] = await cursor.to_list(total)
        return result, total

    async def _update_one(
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
        result: UpdateResult = await self._db[collection_name].update_one(
            query_filter, update, session=self._session
        )
        return result.modified_count

    async def _update_many(
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
        result: UpdateResult = await self._db[collection_name].update_many(
            query_filter, update, session=self._session
        )
        return result.modified_count

    async def _replace_one(
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
        result: UpdateResult = await self._db[collection_name].replace_one(
            query_filter, replacement, session=self._session
        )
        return result.modified_count

    async def _delete_one(self, collection_name: str, query_filter: QueryFilter) -> int:
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
        result: DeleteResult = await self._db[collection_name].delete_one(
            query_filter, session=self._session
        )
        return result.deleted_count

    async def _delete_many(self, collection_name: str, query_filter: QueryFilter) -> int:
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
        result: DeleteResult = await self._db[collection_name].delete_many(
            query_filter, session=self._session
        )
        return result.deleted_count

    async def list_collection_names(self) -> List[str]:
        return cast(List[str], await self._db.list_collection_names())

    async def _rename_collection(self, collection_name: str, new_collection_name: str) -> None:
        """
        Rename collection name

        Parameters
        ----------
        collection_name: str
            From collection name
        new_collection_name: str
            To collection name
        """
        await self._db[collection_name].rename(new_collection_name)

    @asynccontextmanager
    async def _start_transaction(self) -> AsyncIterator[MongoDB]:
        """
        MongoDB transaction session context manager

        Yields
        ------
        AsyncIterator[MongoDB]
            MongoDB object
        """
        try:
            async with await self._client.start_session() as session:
                async with session.start_transaction():
                    self._session = session
                    yield self
        finally:
            if self._session:
                self._session = None
