"""
Module for lookup feature sql generation
"""
from __future__ import annotations

from typing import cast

from dataclasses import dataclass

from sqlglot.expressions import Expression, Select

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import SQLType, quoted_identifier
from featurebyte.query_graph.sql.specs import LookupSpec


@dataclass
class Lookup(TableNode):
    """
    LookupNode SQLNode

    This node has two responsibilities:

    1. Serve as the source of lookup features when sql type is AGGREGATION
    2. Construct post-aggregation expressions involving lookup features when sql type is
       POST_AGGREGATION

    The heavy lifting of lookup join (dimension lookup or SCD lookup) from the source is not done
    here but in LookupAggregator.
    """

    source_node: TableNode
    query_node_type = NodeType.LOOKUP

    @property
    def sql(self) -> Expression:
        return self.source_node.sql

    @classmethod
    def build(cls, context: SQLNodeContext) -> Lookup:

        # Should have only one input
        input_sql_nodes = context.input_sql_nodes
        assert len(input_sql_nodes) == 1

        # That input should be TableNode
        input_node = input_sql_nodes[0]
        assert isinstance(input_node, TableNode)
        source_node = input_node

        if context.sql_type == SQLType.AGGREGATION:
            columns_map = source_node.copy().columns_map
        else:
            assert context.sql_type == SQLType.POST_AGGREGATION
            # Create LookupSpec which determines the internal aggregated result names
            columns_map = {}
            specs = LookupSpec.from_lookup_query_node(
                context.query_node, source_expr=cast(Select, source_node.sql)
            )
            for spec in specs:
                columns_map[spec.feature_name] = quoted_identifier(spec.agg_result_name)

        return Lookup(
            context=context,
            columns_map=columns_map,
            source_node=source_node,
        )
