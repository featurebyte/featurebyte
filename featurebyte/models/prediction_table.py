"""
PredictionTable related model(s)
"""
from __future__ import annotations

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTable


class PredictionTableModel(MaterializedTable):
    """
    PredictionTable is the result of asynchronous prediction features requests
    """

    observation_table_id: PydanticObjectId
    feature_list_id: PydanticObjectId

    class Settings(MaterializedTable.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "prediction_table"
