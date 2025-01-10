"""
This module contains time series table related models.
"""

from typing import ClassVar

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TimeIntervalUnit
from featurebyte.models.base import FeatureByteBaseModel


class TimeInterval(FeatureByteBaseModel):
    """
    Time interval between consecutive records in each series

    See Also
    --------
    - [TimeIntervalUnit](/reference/featurebyte.enum.TimeIntervalUnit/):
        Enumeration of time interval units
    - [create_time_series_table](/reference/featurebyte.api.source_table.SourceTable.create_time_series_table/):
        Create time series table from source table
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TimeInterval")

    unit: TimeIntervalUnit
    value: int
