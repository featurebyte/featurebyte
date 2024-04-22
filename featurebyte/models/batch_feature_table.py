"""
BatchFeatureTable related model(s)
"""

from __future__ import annotations

import pymongo

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTableModel


class BatchFeatureTableModel(MaterializedTableModel):
    """
    BatchFeatureTable is a table that stores the result of asynchronous prediction features requests
    """

    batch_request_table_id: PydanticObjectId
    deployment_id: PydanticObjectId

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
