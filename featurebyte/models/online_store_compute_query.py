"""
AggregationResult document model
"""

from __future__ import annotations

from typing import List, Optional

import pymongo
from pydantic import Field, StrictStr

from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.node.schema import TableDetails


class OnlineStoreComputeQueryModel(FeatureByteCatalogBaseDocumentModel):
    """
    OnlineStoreComputeQueryModel document

    This model tracks a query that computes an aggregation result to be stored in an online store
    table. An aggregation result is the outcome of aggregating a tile table using a user specified
    feature derivation window.

    When a tile is updated in a scheduled task, the related aggregation results are also updated.
    That is done in TileScheduleOnlineStore by retrieving all the OnlineStoreComputeQueryModel
    associated with the aggregation_id and executing the sql queries.
    """

    tile_id: str
    aggregation_id: str
    result_name: str
    result_type: str
    sql: str
    table_name: str
    serving_names: List[StrictStr]
    feature_store_id: Optional[PydanticObjectId] = Field(default=None)
    use_deployed_tile_table: bool = Field(default=False)

    @property
    def warehouse_tables(self) -> list[TableDetails]:
        return [TableDetails(table_name=self.tile_id), TableDetails(table_name=self.table_name)]

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "online_store_compute_query"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("feature_store_id"),
            pymongo.operations.IndexModel("aggregation_id"),
            pymongo.operations.IndexModel("result_name"),
            pymongo.operations.IndexModel("use_deployed_tile_table"),
        ]
        auditable = False
