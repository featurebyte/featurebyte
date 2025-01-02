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
        return f"{self.size}_{self.unit}"

    def is_fixed_size(self) -> bool:
        return self.unit not in {TimeIntervalUnit.MONTH, TimeIntervalUnit.YEAR}

    def to_seconds(self) -> int:
        """
        Convert window to seconds

        Returns
        -------
        int
        """
        assert self.is_fixed_size(), "Only fixed size window can be converted to seconds"
        mapping = {
            TimeIntervalUnit.MINUTE: 60,
            TimeIntervalUnit.HOUR: 3600,
            TimeIntervalUnit.DAY: 86400,
            TimeIntervalUnit.WEEK: 604800,
        }
        return self.size * mapping[self.unit]

    def to_months(self) -> int:
        """
        Convert window to months

        Returns
        -------
        int
        """
        if self.unit == TimeIntervalUnit.MONTH:
            return self.size
        assert (
            self.unit == TimeIntervalUnit.YEAR
        ), "Only month and year window can be converted to months"
        return self.size * 12

    def __hash__(self) -> int:
        return hash(f"{self.size}_{self.unit}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FeatureWindow):
            return False
        return self.size == other.size and self.unit == other.unit
