"""
ObservationTableTileCacheModel
"""

from __future__ import annotations

from typing import List

from pydantic import Field
from pymongo import IndexModel

from featurebyte.models.base import FeatureByteCatalogBaseDocumentModel, PydanticObjectId


class ObservationTableTileCacheModel(FeatureByteCatalogBaseDocumentModel):
    """
    ObservationTableTileCacheModel class

    This model keeps track of the tile tables that have been generated for a given observation
    table.
    """

    observation_table_id: PydanticObjectId
    aggregation_ids: List[str] = Field(default_factory=list)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name = "observation_table_tile_cache"
        unique_constraints = []
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            IndexModel("observation_table_id"),
        ]
        auditable = False
