"""
DeployedTileTableModel class
"""

from typing import List, Optional

import pymongo
from pydantic import Field, StrictStr

from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    UniqueValuesConstraint,
)
from featurebyte.models.tile_compute_query import TileComputeQuery
from featurebyte.models.tile_registry import BackfillMetadata, LastRunMetadata
from featurebyte.query_graph.node.schema import TableDetails


class TileIdentifier(FeatureByteBaseModel):
    """
    TileTableIdentifier class
    """

    tile_id: str
    aggregation_id: str


class DeployedTileTableModel(FeatureByteCatalogBaseDocumentModel):
    """
    DeployedTileTableModel class

    This represents a deployed tile table associated with
    * One specific tile scheduled task
    * One or more enabled deployments

    It contains a sql query that computes tile table columns for a tile table that is updated
    periodically. The tile table name is not the same as tile_id since each deployed tile table can
    be associated with more than one tile_id (after combining compatible tile queries during
    deployment enablement).

    This will eventually replace TileModel for new deployments.
    """

    table_name: str
    tile_identifiers: list[TileIdentifier]

    tile_compute_query: TileComputeQuery
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
        return [TableDetails(table_name=self.table_name)]

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "deployed_tile_table"
        unique_constraints: List[UniqueValuesConstraint] = []

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("table_name"),
            pymongo.operations.IndexModel("tile_identifiers.tile_id"),
            pymongo.operations.IndexModel("tile_identifiers.aggregation_id"),
        ]
