"""
TileModel document model
"""
from __future__ import annotations

from typing import List, Optional

from datetime import datetime

import pymongo
from pydantic import BaseModel, StrictStr

from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class LastTileMetadata(BaseModel):
    """
    LastTileMetadata class
    """

    start_date: datetime
    index: int


class TileModel(FeatureByteCatalogBaseDocumentModel):
    """
    TileModel document
    """

    feature_store_id: PydanticObjectId
    tile_id: StrictStr
    aggregation_id: StrictStr

    tile_sql: StrictStr
    entity_column_names: List[StrictStr]
    value_column_names: List[StrictStr]
    value_column_types: List[StrictStr]

    frequency_minute: int
    time_modulo_frequency_second: int
    blind_spot_second: int

    last_tile_metadata_online: Optional[LastTileMetadata]
    last_tile_metadata_offline: Optional[LastTileMetadata]

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "tile"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
            UniqueValuesConstraint(
                fields=("tile_id", "aggregation_id"),
                conflict_fields_signature={
                    "tile_id": ["tile_id"],
                    "aggregation_id": ["aggregation_id"],
                },
                resolution_signature=None,
            ),
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("feature_store_id"),
            pymongo.operations.IndexModel("tile_id"),
            pymongo.operations.IndexModel("aggregation_id"),
        ]


class TileUpdate(BaseDocumentServiceUpdateSchema):
    """
    Schema for TileUpdate
    """

    last_tile_metadata_online: Optional[LastTileMetadata]
    last_tile_metadata_offline: Optional[LastTileMetadata]
