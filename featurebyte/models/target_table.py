"""
TargetTableModel
"""
from __future__ import annotations

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.models.materialized_table import MaterializedTableModel


class TargetTableModel(BaseFeatureOrTargetTableModel):
    """
    TargetTable is the result of asynchronous target requests
    """

    target_id: PydanticObjectId

    class Settings(MaterializedTableModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "target_table"
