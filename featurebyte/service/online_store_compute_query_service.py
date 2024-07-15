"""
OnlineStoreComputeQueryService class
"""

from __future__ import annotations

from typing import AsyncIterator

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.online_store_compute_query import OnlineStoreComputeQueryModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService


class OnlineStoreComputeQueryService(
    BaseDocumentService[
        OnlineStoreComputeQueryModel,
        OnlineStoreComputeQueryModel,
        BaseDocumentServiceUpdateSchema,
    ]
):
    """
    OnlineStoreComputeQueryService

    The collection keeps track of currently active queries that are being run periodically to update
    online store tables.
    """

    document_class = OnlineStoreComputeQueryModel

    async def list_by_aggregation_id(
        self, aggregation_id: str
    ) -> AsyncIterator[OnlineStoreComputeQueryModel]:
        """
        List all documents by aggregation_id

        Parameters
        ----------
        aggregation_id: str
            Aggregation id

        Yields
        ------
        list[OnlineStoreComputeQueryModel]
            List of OnlineStoreComputeQueryModel
        """
        async for model in self.list_documents_iterator(
            query_filter={"aggregation_id": aggregation_id}
        ):
            yield model

    async def list_by_result_names(
        self, result_names: list[str]
    ) -> AsyncIterator[OnlineStoreComputeQueryModel]:
        """
        List all documents by aggregation result names

        Parameters
        ----------
        result_names: list[str]
            Result names

        Yields
        ------
        list[OnlineStoreComputeQueryModel]
            List of OnlineStoreComputeQueryModel
        """
        async for model in self.list_documents_iterator(
            query_filter={"result_name": {"$in": result_names}}
        ):
            yield model

    async def delete_by_result_name(self, result_name: str) -> None:
        """
        Delete a document by result_name

        Parameters
        ----------
        result_name: str
            Result name

        Raises
        ------
        DocumentNotFoundError
            If there is no document with the given result_name
        """
        query_filter = {"result_name": result_name}
        async for doc in self.list_documents_as_dict_iterator(
            query_filter=query_filter, projection={"_id": 1}
        ):
            document_id = doc["_id"]
            break
        else:
            raise DocumentNotFoundError("Aggregation result name not found")
        await self.delete_document(document_id)
