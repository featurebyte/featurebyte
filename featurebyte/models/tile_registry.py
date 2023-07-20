"""
TileModel document model
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime

import pymongo
from pydantic import BaseModel, Field, StrictStr, root_validator

from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class LastRunMetadata(BaseModel):
    """
    LastRunMetadata class

    Metadata of the latest run of tile generation

    tile_end_date: datetime
        The tile end date used in the latest run of tile generation. This can be used as the tile
        start date when running tile generation again.
    index: int
        Tile index corresponding to the tile end date
    """

    @root_validator(pre=True)
    @classmethod
    def _convert_start_date(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # DEV-556: backward compatibility after renaming field
        if values.get("start_date"):
            values["tile_end_date"] = values["start_date"]
        return values

    tile_end_date: datetime
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

    frequency_minute: int = Field(gt=0)
    time_modulo_frequency_second: int = Field(ge=0)
    blind_spot_second: int = Field(ge=0)

    last_run_metadata_online: Optional[LastRunMetadata]
    last_run_metadata_offline: Optional[LastRunMetadata]

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
        auditable = False


class TileUpdate(BaseDocumentServiceUpdateSchema):
    """
    Schema for TileUpdate
    """

    last_run_metadata_online: Optional[LastRunMetadata]
    last_run_metadata_offline: Optional[LastRunMetadata]
