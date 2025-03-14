"""
BatchFeatureTable related model(s)
"""

from __future__ import annotations

from typing import Optional

import pymongo
from pydantic import Field

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.batch_request_table import BatchRequestInput
from featurebyte.models.materialized_table import MaterializedTableModel


class BatchFeatureTableModel(MaterializedTableModel):
    """
    BatchFeatureTable is a table that stores the result of asynchronous prediction features requests
    """

    batch_request_table_id: Optional[PydanticObjectId]
    request_input: Optional[BatchRequestInput] = Field(default=None)
    deployment_id: PydanticObjectId
    parent_batch_feature_table_id: Optional[PydanticObjectId] = Field(default=None)

    class Settings(MaterializedTableModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "batch_feature_table"

        indexes = MaterializedTableModel.Settings.indexes + [
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
