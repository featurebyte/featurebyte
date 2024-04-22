"""
SQL generation for forward as-at aggregation
"""

from __future__ import annotations

from featurebyte.query_graph.sql.aggregator.base_asat import BaseAsAtAggregator
from featurebyte.query_graph.sql.offset import OffsetDirection
from featurebyte.query_graph.sql.specifications.forward_aggregate_asat import (
    ForwardAggregateAsAtSpec,
)


class ForwardAsAtAggregator(BaseAsAtAggregator[ForwardAggregateAsAtSpec]):
    """
    ForwardAsAtAggregation is responsible for generating SQL for as at aggregation for target
    """

    @classmethod
    def get_offset_direction(cls) -> OffsetDirection:
        return OffsetDirection.FORWARD
