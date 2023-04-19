"""
BatchFeatureTable related model(s)
"""
from __future__ import annotations

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTable


class BatchFeatureTableModel(MaterializedTable):
    """
    BatchFeatureTable is a table that stores the result of asynchronous prediction features requests
    """

    observation_table_id: PydanticObjectId
    feature_list_id: PydanticObjectId

    class Settings(MaterializedTable.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "batch_feature_table"
