"""
MigrationServiceMixin class
"""
from __future__ import annotations

from typing import Any, Optional, Protocol

from abc import abstractmethod

from featurebyte.models.persistent import QueryFilter
from featurebyte.persistent.base import Persistent


class MigrationServiceMixin(Protocol):
    """MigrationServiceMixin class"""

    persistent: Persistent

    @property
    @abstractmethod
    def collection_name(self) -> str:
        """
        Collection name

        Returns
        -------
        Collection name
        """

    @abstractmethod
    def _construct_list_query_filter(
        self, query_filter: Optional[dict[str, Any]] = None, **kwargs: Any
    ) -> QueryFilter:
        ...

    @classmethod
    def migrate_document_record(cls, record: dict[str, Any]) -> dict[str, Any]:
        """
        Migrate older document record to the current document record format

        Parameters
        ----------
        record: dict[str, Any]
            Older document record

        Returns
        -------
        dict[str, Any]
            Record in newer format
        """
        return cls.document_class(**record).dict(by_alias=True)  # type: ignore

    async def migrate_all_records(
        self,
        query_filter: Optional[dict[str, Any]] = None,
        page_size: int = 10,
    ) -> None:
        """
        Migrate all records in this service's collection & audit collection

        Parameters
        ----------
        query_filter: Optional[dict[str, Any]]
            Query filter used to filter the documents used for migration
        page_size: int
            Page size
        """
        # migrate all records and audit records
        if query_filter is None:
            query_filter = dict(self._construct_list_query_filter())
        to_iterate, page = True, 1
        while to_iterate:
            docs, total = await self.persistent.find(
                collection_name=self.collection_name,
                query_filter=query_filter,
                page=page,
                page_size=page_size,
            )
            for doc in docs:
                await self.persistent.migrate_record(
                    collection_name=self.collection_name,
                    document=doc,
                    migrate_func=self.migrate_document_record,
                )

            to_iterate = bool(total > (page * page_size))
