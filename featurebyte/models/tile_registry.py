"""
TileModel document model
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, List, Optional

import pymongo
from pydantic import BaseModel, Field, StrictStr, model_validator

from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class LastRunMetadata(FeatureByteBaseModel):
    """
    LastRunMetadata class

    Metadata of the latest run of tile generation

    tile_end_date: datetime
        The tile end date used in the latest run of tile generation. This can be used as the tile
        start date when running tile generation again.
    index: int
        Tile index corresponding to the tile end date
    """

    tile_end_date: datetime
    index: int

    @model_validator(mode="before")
    @classmethod
    def _convert_start_date(cls, values: Any) -> Any:
        # DEV-556: backward compatibility after renaming field
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        if values.get("start_date"):
            values["tile_end_date"] = values["start_date"]
        return values


class BackfillMetadata(FeatureByteBaseModel):
    """
    BackfillMetadata class

    Metadata of the tile backfill process when enabling a deployment
    """

    start_date: datetime


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

    last_run_metadata_online: Optional[LastRunMetadata] = Field(default=None)
    last_run_metadata_offline: Optional[LastRunMetadata] = Field(default=None)
    backfill_metadata: Optional[BackfillMetadata] = Field(default=None)

    @property
    def warehouse_tables(self) -> list[TableDetails]:
        return [TableDetails(table_name=self.tile_id)]

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

    last_run_metadata_online: Optional[LastRunMetadata] = Field(default=None)
    last_run_metadata_offline: Optional[LastRunMetadata] = Field(default=None)
    backfill_metadata: Optional[BackfillMetadata] = Field(default=None)
