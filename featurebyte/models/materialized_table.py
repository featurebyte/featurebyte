"""
MaterializedTable model
"""
from __future__ import annotations

from typing import List

import pymongo

from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import ColumnSpec


class MaterializedTableModel(FeatureByteCatalogBaseDocumentModel):
    """
    MaterializedTableModel represents a table that has been materialized and stored in feature store
    database.

    location: TabularSource
        The table that stores the materialized data
    columns_info: List[ColumnSpec]
        The columns in the table
    """

    location: TabularSource
    columns_info: List[ColumnSpec]
    num_rows: int

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
