"""
This module contains time series table related models.
"""

from typing import ClassVar

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TimeIntervalUnit
from featurebyte.models.base import FeatureByteBaseModel


class TimeInterval(FeatureByteBaseModel):
    """
    Represents the time interval between consecutive records in each series.

    This class is used to define the consistent time gap expected between consecutive
    records within a time series dataset. Specifying the time interval helps in
    validating the data and ensuring uniformity across the time series.

    Parameters
    ----------
    unit : TimeIntervalUnit
        The unit of time for the interval. Common units include (not exhaustive):
        - `TimeIntervalUnit.MINUTE`
        - `TimeIntervalUnit.HOUR`
        - `TimeIntervalUnit.DAY`
    value : int
        The numerical value representing the quantity of the specified time unit.
        For example, a `unit` of `"day"` and a `value` of `1` indicates a daily interval.

    See Also
    --------
    - [TimeIntervalUnit](/reference/featurebyte.enum.TimeIntervalUnit/):
        Enum class for time interval units.
    - [create_time_series_table](/reference/featurebyte.api.source_table.SourceTable.create_time_series_table/):
        Create time series table from source table
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TimeInterval")

    unit: TimeIntervalUnit
    value: int
