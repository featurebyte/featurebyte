"""
Model for Managed View.
"""

from typing import List

import pymongo
from pydantic import StrictStr

from featurebyte.models.base import FeatureByteBaseDocumentModel, UniqueValuesConstraint
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TabularSource


class ManagedViewModel(FeatureByteBaseDocumentModel):
    """
    Model for Managed View.
    """

    sql: StrictStr
    tabular_source: TabularSource
    columns_info: List[ColumnInfo]

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name = "managed_view"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
        ]

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ]
        ]
