"""
ObservationTableTileCacheService
"""

from typing import List

from bson import ObjectId

from featurebyte.models.observation_table_tile_cache import ObservationTableTileCacheModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService


class ObservationTableTileCacheService(
    BaseDocumentService[
        ObservationTableTileCacheModel,
        ObservationTableTileCacheModel,
        BaseDocumentServiceUpdateSchema,
    ]
):
    """
    ObservationTableTileCacheDocumentService class
    """

    document_class = ObservationTableTileCacheModel

    async def add_aggregation_ids_for_observation_table(
        self, observation_table_id: ObjectId, aggregation_ids: List[str]
    ) -> None:
        """
        Update the cache for an observation table

        Parameters
        ----------
        observation_table_id: ObjectId
            Observation table ID
        aggregation_ids: List[str]
            List of aggregation IDs
        """
        query_filter = {"observation_table_id": observation_table_id}
        async for doc in self.list_documents_as_dict_iterator(query_filter=query_filter):
            document_id = doc["_id"]
            break
        else:
            created_doc = await self.create_document(
                ObservationTableTileCacheModel(observation_table_id=observation_table_id)
            )
            document_id = created_doc.id
        await self.update_documents(
            query_filter={"_id": document_id},
            update={"$addToSet": {"aggregation_ids": {"$each": aggregation_ids}}},
        )

    async def get_non_cached_aggregation_ids(
        self, observation_table_id: ObjectId, aggregation_ids: List[str]
    ) -> List[str]:
        """
        Get the aggregation IDs that are not cached

        Parameters
        ----------
        observation_table_id: ObjectId
            Observation table ID
        aggregation_ids: List[str]
            List of aggregation IDs

        Returns
        -------
        List[str]
        """
        query_filter = {"observation_table_id": observation_table_id}
        cached_aggregation_ids = set()
        async for doc in self.list_documents_as_dict_iterator(query_filter=query_filter):
            cached_aggregation_ids.update(doc["aggregation_ids"])
        return [agg_id for agg_id in aggregation_ids if agg_id not in cached_aggregation_ids]
