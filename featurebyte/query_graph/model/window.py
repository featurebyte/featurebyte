"""
Models for feature derivation window
"""

from functools import total_ordering
from typing import ClassVar

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TimeIntervalUnit
from featurebyte.models.base import FeatureByteBaseModel


@total_ordering
class CalendarWindow(FeatureByteBaseModel):
    """
    Calendar window for feature derivation

    See Also
    --------
    - [TimeIntervalUnit](/reference/featurebyte.enum.TimeIntervalUnit/):
        Enumeration of time interval units
    - [aggregate_over](/reference/featurebyte.api.groupby.GroupBy.aggregate_over/):
        Window aggregation specification
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.CalendarWindow")

    unit: TimeIntervalUnit
    size: int

    def to_string(self) -> str:
        return f"{self.size}_{self.unit}"

    def is_fixed_size(self) -> bool:
        return self.unit not in {
            TimeIntervalUnit.MONTH,
            TimeIntervalUnit.QUARTER,
            TimeIntervalUnit.YEAR,
        }

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
        elif self.unit == TimeIntervalUnit.QUARTER:
            return self.size * 3
        assert (
            self.unit == TimeIntervalUnit.YEAR
        ), "Only month, quarter, and year window can be converted to months"
        return self.size * 12

    def __hash__(self) -> int:
        return hash(f"{self.size}_{self.unit}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CalendarWindow):
            return False
        if self.is_fixed_size() and other.is_fixed_size():
            return self.to_seconds() == other.to_seconds()
        elif not self.is_fixed_size() and not other.is_fixed_size():
            return self.to_months() == other.to_months()
        return False

    def __lt__(self, other: "CalendarWindow") -> bool:
        if self.is_fixed_size() != other.is_fixed_size():
            raise ValueError("Cannot compare a fixed size window with a non-fixed size window")
        if self.is_fixed_size():
            return self.to_seconds() < other.to_seconds()
        return self.to_months() < other.to_months()
