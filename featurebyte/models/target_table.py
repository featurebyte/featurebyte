"""
TargetTableModel
"""
from __future__ import annotations

from typing import Optional

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTableModel


class TargetTableModel(MaterializedTableModel):
    """
    TargetTable is the result of asynchronous target requests
    """

    observation_table_id: Optional[PydanticObjectId]
    target_id: PydanticObjectId

    class Settings(MaterializedTableModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "target_table"
