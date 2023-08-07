"""
SQL generation for lookup targets
"""
from __future__ import annotations

from typing import Optional

from sqlglot.expressions import Select

from featurebyte.query_graph.node.generic import SCDLookupParameters
from featurebyte.query_graph.sql.aggregator.base_lookup import BaseLookupAggregator
from featurebyte.query_graph.sql.scd_helper import OffsetDirection, Table, get_scd_join_expr
from featurebyte.query_graph.sql.specifications.lookup_target import LookupTargetSpec


class LookupTargetAggregator(BaseLookupAggregator[LookupTargetSpec]):
    """
    LookupTargetAggregator is responsible for generating SQL for lookup targets
    """

    def get_scd_join_expr_for_lookup(
        self, left_table: Table, right_table: Table, scd_parameters: SCDLookupParameters
    ) -> Select:
        return get_scd_join_expr(
            left_table,
            right_table,
            join_type="left",
            adapter=self.adapter,
            offset=scd_parameters.offset,
            offset_direction=OffsetDirection.FORWARD,
        )

    def get_forward_point_in_time_offset(self, base_lookup_spec: LookupTargetSpec) -> Optional[str]:
        return base_lookup_spec.offset
