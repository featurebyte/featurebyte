"""
AggregateAsAtSpec
"""

from __future__ import annotations

from dataclasses import dataclass

from featurebyte.query_graph.sql.specifications.base_aggregate_asat import BaseAggregateAsAtSpec
from featurebyte.query_graph.sql.specs import AggregationType


@dataclass
class AggregateAsAtSpec(BaseAggregateAsAtSpec):
    """
    As-at aggregation specification
    """

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.AS_AT
