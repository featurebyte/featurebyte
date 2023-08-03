"""
Lookup target spec
"""
from __future__ import annotations

from dataclasses import dataclass

from featurebyte.query_graph.sql.specifications.base_lookup import BaseLookupSpec
from featurebyte.query_graph.sql.specs import AggregationType


@dataclass
class LookupTargetSpec(BaseLookupSpec):
    """
    LookupTargetSpec contains all information required to generate sql for a lookup target feature
    """

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.LOOKUP_TARGET
