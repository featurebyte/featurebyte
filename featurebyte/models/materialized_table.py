"""
MaterializedTable model
"""

from __future__ import annotations

from typing import List, Optional

import pymongo
from pydantic import Field

from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import ColumnSpec, TableDetails


class ColumnSpecWithEntityId(ColumnSpec):
    """
    Column spec with entity ID.

    We only add entity ID here so that we don't have to add it to all other callers which use ColumnSpec too.
    """

    entity_id: Optional[PydanticObjectId] = Field(default=None)


class MaterializedTableModel(FeatureByteCatalogBaseDocumentModel):
    """
    MaterializedTableModel represents a table that has been materialized and stored in feature store
    database.

    location: TabularSource
        The table that stores the materialized data
    columns_info: List[ColumnSpec]
        The columns in the table
    is_view: bool
        Whereas table is represented by a view physically
    """

    location: TabularSource
    columns_info: List[ColumnSpecWithEntityId]
    num_rows: int
    is_view: bool = Field(default=False)

    @property
    def warehouse_tables(self) -> list[TableDetails]:
        return [self.location.table_details]

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("location.feature_store_id"),
        ]
