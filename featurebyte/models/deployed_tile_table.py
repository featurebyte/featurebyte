"""
DeployedTileTableModel class
"""

from typing import List

import pymongo

from featurebyte.models.base import FeatureByteCatalogBaseDocumentModel, UniqueValuesConstraint


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
    """

    table_name: str
    tile_ids: list[str]
    aggregation_ids: list[str]

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "deployed_tile_table"
        unique_constraints: List[UniqueValuesConstraint] = []

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("table_name"),
            pymongo.operations.IndexModel("tile_ids"),
            pymongo.operations.IndexModel("aggregation_ids"),
        ]
