"""
DeployedTileTableModel class
"""

from typing import List, Optional

import pymongo
from pydantic import Field, StrictStr

from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)
from featurebyte.models.tile import OnDemandTileTable, TileSpec
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

    feature_store_id: PydanticObjectId
    table_name: str
    tile_identifiers: list[TileIdentifier]

    tile_compute_query: TileComputeQuery
    entity_column_names: List[StrictStr]
    value_column_names: List[StrictStr]
    value_column_types: List[StrictStr]
    value_by_column: Optional[StrictStr]

    frequency_minute: int = Field(gt=0)
    time_modulo_frequency_second: int = Field(ge=0)
    blind_spot_second: int = Field(ge=0)

    last_run_metadata_online: Optional[LastRunMetadata] = Field(default=None)
    last_run_metadata_offline: Optional[LastRunMetadata] = Field(default=None)
    backfill_metadata: Optional[BackfillMetadata] = Field(default=None)

    @property
    def warehouse_tables(self) -> list[TableDetails]:
        return [TableDetails(table_name=self.table_name)]

    @property
    def aggregation_ids(self) -> List[str]:
        """
        Get the aggregation IDs associated with this deployed tile table

        Returns
        -------
        List[str]
            List of aggregation IDs
        """
        return [tile_identifier.aggregation_id for tile_identifier in self.tile_identifiers]

    @property
    def tile_ids(self) -> List[str]:
        """
        Get the tile IDs associated with this deployed tile table

        Returns
        -------
        List[str]
            List of tile IDs
        """
        return [tile_identifier.tile_id for tile_identifier in self.tile_identifiers]

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

        auditable = False

    def to_tile_spec(self) -> TileSpec:
        """
        Returns a TileSpec object for this deployed tile table

        Returns
        -------
        TileSpec
            TileSpec object representing the deployed tile table
        """
        entity_column_names = self.entity_column_names[:]
        if self.value_by_column is not None:
            entity_column_names.append(self.value_by_column)
        return TileSpec(
            time_modulo_frequency_second=self.time_modulo_frequency_second,
            blind_spot_second=self.blind_spot_second,
            frequency_minute=self.frequency_minute,
            tile_compute_query=self.tile_compute_query,
            entity_column_names=entity_column_names,
            value_column_names=self.value_column_names,
            value_column_types=self.value_column_types,
            tile_id=self.table_name,
            aggregation_id=self.tile_identifiers[0].aggregation_id,
            feature_store_id=self.feature_store_id,
        )


class DeployedTileTableInfo(FeatureByteBaseModel):
    """
    DeployedTileTableInfo contains information about deployed tile tables to be used when generating
    internal online store compute queries
    """

    deployed_tile_tables: List[DeployedTileTableModel] = Field(default_factory=list)

    @property
    def on_demand_tile_tables(self) -> List[OnDemandTileTable]:
        """
        Get a list of OnDemandTileTable objects for use in FeatureExecutionPlanner

        Returns
        -------
        List[OnDemandTileTable]
            List of OnDemandTileTable
        """
        on_demand_tile_tables = [
            OnDemandTileTable(tile_table_id=tile_id, on_demand_table_name=table_name)
            for tile_id, table_name in self.tile_table_mapping.items()
        ]
        return on_demand_tile_tables

    @property
    def tile_table_mapping(self) -> dict[str, str]:
        """
        Get a mapping of tile_id to the actual tile table name

        Returns
        -------
        dict[str, str]
        """
        mapping = {}
        for doc in self.deployed_tile_tables:
            for tile_id in doc.tile_ids:
                if tile_id not in mapping:
                    mapping[tile_id] = doc.table_name
        return mapping
