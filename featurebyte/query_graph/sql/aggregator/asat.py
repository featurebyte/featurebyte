"""
SQL generation for as-at aggregation
"""

from __future__ import annotations

from featurebyte.query_graph.sql.aggregator.base_asat import BaseAsAtAggregator
from featurebyte.query_graph.sql.offset import OffsetDirection
from featurebyte.query_graph.sql.specifications.aggregate_asat import AggregateAsAtSpec


class AsAtAggregator(BaseAsAtAggregator[AggregateAsAtSpec]):
    """
    AsAtAggregation is responsible for generating SQL for as at aggregation
    """

    @classmethod
    def get_offset_direction(cls) -> OffsetDirection:
        return OffsetDirection.BACKWARD
