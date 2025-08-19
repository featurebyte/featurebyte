"""
WarehouseTableModel
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import pymongo

from featurebyte.models.base import FeatureByteBaseModel, FeatureByteCatalogBaseDocumentModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class WarehouseTableUpdate(FeatureByteBaseModel):
    """
    WarehouseTable update schema
    """

    cleanup_failed_count: Optional[int] = None


class WarehouseTableServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    WarehouseTableService update schema
    """

    cleanup_failed_count: Optional[int] = None


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
    cleanup_failed_count: int = 0

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
