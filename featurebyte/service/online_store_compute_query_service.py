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

    async def list_by_aggregation_ids(
        self, aggregation_ids: list[str], use_deployed_tile_table: bool = True
    ) -> AsyncIterator[OnlineStoreComputeQueryModel]:
        """
        List all documents by aggregation_id

        Parameters
        ----------
        aggregation_ids: list[str]
            Aggregation ids
        use_deployed_tile_table: bool
            Whether to retrieve sql queries that uses deployed tile table

        Yields
        ------
        list[OnlineStoreComputeQueryModel]
            List of OnlineStoreComputeQueryModel
        """
        async for model in self.list_documents_iterator(
            query_filter={"aggregation_id": {"$in": aggregation_ids}}
        ):
            if (model.use_deployed_tile_table and not use_deployed_tile_table) or (
                not model.use_deployed_tile_table and use_deployed_tile_table
            ):
                continue
            yield model

    async def list_by_result_names(
        self, result_names: list[str], use_deployed_tile_table: bool = True
    ) -> AsyncIterator[OnlineStoreComputeQueryModel]:
        """
        List all documents by aggregation result names

        Parameters
        ----------
        result_names: list[str]
            Result names
        use_deployed_tile_table: bool
            Whether to retrieve sql queries that uses deployed tile table

        Yields
        ------
        list[OnlineStoreComputeQueryModel]
            List of OnlineStoreComputeQueryModel
        """
        async for model in self.list_documents_iterator(
            query_filter={"result_name": {"$in": result_names}}
        ):
            if (model.use_deployed_tile_table and not use_deployed_tile_table) or (
                not model.use_deployed_tile_table and use_deployed_tile_table
            ):
                continue
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
