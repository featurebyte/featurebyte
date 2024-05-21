"""
Target table model
"""

from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.models.observation_table import ObservationTableModel


class TargetTableModel(ObservationTableModel, BaseFeatureOrTargetTableModel):
    """
    Base Feature Or Target table model mixin for shared properties
    """
