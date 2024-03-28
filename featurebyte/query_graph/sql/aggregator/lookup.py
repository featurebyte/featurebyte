"""
SQL generation for lookup features
"""

from __future__ import annotations

from sqlglot.expressions import Select

from featurebyte.query_graph.node.generic import SCDLookupParameters
from featurebyte.query_graph.sql.aggregator.base_lookup import BaseLookupAggregator
from featurebyte.query_graph.sql.scd_helper import Table, get_scd_join_expr
from featurebyte.query_graph.sql.specifications.lookup import LookupSpec


class LookupAggregator(BaseLookupAggregator[LookupSpec]):
    """
    LookupAggregator is responsible for generating SQL for lookup features
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
        )
