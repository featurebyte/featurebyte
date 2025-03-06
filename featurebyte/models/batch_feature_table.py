"""
BatchFeatureTable related model(s)
"""

from __future__ import annotations

from typing import List, Optional

import pymongo
from pydantic import Field

from featurebyte.models.base import (
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.batch_request_table import BatchRequestInput
from featurebyte.models.materialized_table import MaterializedTableModel


class BatchFeatureTableModel(MaterializedTableModel):
    """
    BatchFeatureTable is a table that stores the result of asynchronous prediction features requests
    """

    batch_request_table_id: Optional[PydanticObjectId]
    request_input: Optional[BatchRequestInput] = Field(default=None)
    deployment_id: PydanticObjectId
    parent_batch_feature_table_name: Optional[str] = Field(default=None)

    class Settings(MaterializedTableModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "batch_feature_table"

        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name", "deployment_id"),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

        indexes = MaterializedTableModel.Settings.indexes + [
            pymongo.operations.IndexModel("deployment_id"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
