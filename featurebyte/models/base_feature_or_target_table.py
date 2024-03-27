"""
Base Feature Or Target table model
"""

from typing import Optional

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTableModel


class BaseFeatureOrTargetTableModel(MaterializedTableModel):
    """
    Base Feature Or Target table model for shared properties
    """

    observation_table_id: Optional[PydanticObjectId]
