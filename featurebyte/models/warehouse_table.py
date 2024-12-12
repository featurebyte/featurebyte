"""
WarehouseTableModel
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import pymongo

from featurebyte.models.base import FeatureByteCatalogBaseDocumentModel
from featurebyte.query_graph.model.common_table import TabularSource


class WarehouseTableModel(FeatureByteCatalogBaseDocumentModel):
    """
    WarehouseTableModel class

    Represents a catalog specific table in the warehouse with optional metadata.

    tag is an optional string to identify a collection of tables, such as all temporary tile tables
    created for the purpose of creating a historical feature table, in which case the tag could be
    derived based on the historical feature table id.

    expires_at is an optional datetime to indicate when the table should be deleted for clean up
    purpose.
    """

    location: TabularSource
    tag: Optional[str] = None
    expires_at: Optional[datetime] = None

    @property
    def warehouse_tables(self) -> list[Any]:
        return [self.location.table_details]

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "warehouse_table"
        unique_constraints = []
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("location"),
            pymongo.operations.IndexModel("tag"),
        ]
        auditable = False
