"""
Persistent storage using MongoDB
"""

from __future__ import annotations

import asyncio
import copy
from asyncio import iscoroutine
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Tuple, Union, cast
from uuid import UUID

import pymongo
from bson import ObjectId
from bson.errors import InvalidId
from motor.core import AgnosticCursor
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.results import DeleteResult, InsertManyResult, InsertOneResult, UpdateResult

from featurebyte.models.persistent import Document, DocumentUpdate, QueryFilter
from featurebyte.persistent.base import DuplicateDocumentError, Persistent, SortDir

MONGODB_CLIENT = None


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

        if not client:
            # use global client to enforce connection throttling
            global MONGODB_CLIENT
            if MONGODB_CLIENT is None:
                MONGODB_CLIENT = AsyncIOMotorClient(uri, uuidRepresentation="standard")

            # set client to global client
            client = MONGODB_CLIENT

        self._database = database
        self._client = client
        self._db = self._client[self._database]
        self._session: Any = None
        # ensure client uses current loop
        if hasattr(self._client, "get_io_loop"):
            self._client.get_io_loop = asyncio.get_running_loop

    async def _insert_one(self, collection_name: str, document: Document) -> Union[ObjectId, UUID]:
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
        Union[ObjectId, UUID]
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
            try:
                return ObjectId(result.inserted_id)
            except InvalidId:
                return UUID(result.inserted_id)
        except pymongo.errors.DuplicateKeyError as exc:
            raise DuplicateDocumentError() from exc

    async def _insert_many(
        self, collection_name: str, documents: Iterable[Document]
    ) -> list[Union[ObjectId, UUID]]:
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
        list[Union[ObjectId, UUID]]
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
        self,
        collection_name: str,
        query_filter: QueryFilter,
        projection: Optional[dict[str, Any]] = None,
    ) -> Optional[Document]:
        """
        Find one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        projection: Optional[dict[str, Any]]
            Fields to project

        Returns
        -------
        Optional[DocumentType]
            Retrieved document
        """
        result: Optional[Document] = await self._db[collection_name].find_one(
            filter=query_filter, projection=projection, session=self._session
        )
        return result

    async def _find(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        projection: Optional[dict[str, Any]] = None,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
        page: int = 1,
        page_size: int = 0,
    ) -> tuple[Iterable[Document], int]:
        """
        Find all records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        projection: Optional[dict[str, Any]]
            Fields to project
        sort_by: Optional[list[tuple[str, SortDir]]]
            Columns and direction to sort by
        page: int
            Page number for pagination
        page_size: int
            Page size (0 to return all records)

        Returns
        -------
        tuple[Iterable[Document], int]
            Retrieved documents and total count
        """
        cursor: AgnosticCursor = cast(
            AgnosticCursor,
            await self._get_iterator(collection_name, query_filter, None, projection, sort_by),
        )
        total = await self._db[collection_name].count_documents(query_filter, session=self._session)

        if page_size > 0:
            skips = page_size * (page - 1)
            cursor = cursor.skip(skips).limit(page_size)

        result: list[Document] = await cursor.to_list(total)
        return result, total

    async def _get_iterator(
        self,
        collection_name: str,
        query_filter: QueryFilter,
        pipeline: Optional[list[dict[str, Any]]] = None,
        projection: Optional[dict[str, Any]] = None,
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
    ) -> AsyncIterator[Document]:
        """
        Get iterator on records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        query_filter: QueryFilter
            Conditions to filter on
        pipeline: Optional[list[dict[str, Any]]]
            Pipeline to execute
        projection: Optional[dict[str, Any]]
            Fields to project
        sort_by: Optional[list[tuple[str, SortDir]]]
            Columns and direction to sort by

        Returns
        -------
        AsyncIterator[Document]
            Retrieved documents
        """
        if sort_by:
            sort = OrderedDict([
                (str(sort_key), pymongo.ASCENDING if sort_dir == "asc" else pymongo.DESCENDING)
                for sort_key, sort_dir in sort_by
            ])
            if "_id" not in sort:
                sort["_id"] = pymongo.DESCENDING  # break ties using _id
        else:
            sort = None

        if pipeline:
            pipeline = copy.deepcopy(pipeline)
            if query_filter:
                pipeline.insert(0, {"$match": query_filter})
            if sort:
                pipeline.append({"$sort": sort})
            cursor = self._db[collection_name].aggregate(pipeline, session=self._session)
        else:
            cursor = self._db[collection_name].find(
                filter=query_filter, projection=projection, session=self._session
            )
            if sort:
                cursor = cursor.sort(sort.items())

        return cast(AsyncIterator[Document], cursor)

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

    async def list_collection_names(self) -> list[str]:
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

    async def aggregate_find(
        self,
        collection_name: str,
        pipeline: List[Dict[str, Any]],
        sort_by: Optional[list[tuple[str, SortDir]]] = None,
        page: int = 1,
        page_size: int = 0,
        **kwargs: Any,
    ) -> Tuple[Iterable[Document], int]:
        output_pipeline: List[Dict[str, Any]] = []

        if sort_by:
            sort = {
                str(sort_key): pymongo.ASCENDING if sort_dir == "asc" else pymongo.DESCENDING
                for sort_key, sort_dir in sort_by
            }
            sort["_id"] = pymongo.DESCENDING  # break ties using _id
            output_pipeline.append({"$sort": sort})

        if page_size > 0:
            output_pipeline.extend([{"$skip": page_size * (page - 1)}, {"$limit": page_size}])

        pipeline = pipeline + [
            {"$facet": {"result": output_pipeline, "total": [{"$count": "count"}]}}
        ]
        result = self._db[collection_name].aggregate(pipeline, session=self._session).next()
        if iscoroutine(result):
            output = await result
        else:
            output = result
        total = 0 if not output["total"] else output["total"][0]["count"]
        return output["result"], total
