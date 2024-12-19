"""
Models for feature derivation window
"""

from featurebyte.enum import TimeIntervalUnit
from featurebyte.models.base import FeatureByteBaseModel


class FeatureWindow(FeatureByteBaseModel):
    """
    Window for feature derivation
    """

    unit: TimeIntervalUnit
    size: int

    def to_string(self) -> str:
        return f"{self.size} {self.unit}"
