"""
Module for lookup feature sql generation
"""
from __future__ import annotations

from typing import cast

from dataclasses import dataclass

from sqlglot.expressions import Expression, Select

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.ast.generic import Project
from featurebyte.query_graph.sql.common import SQLType, quoted_identifier
from featurebyte.query_graph.sql.specs import LookupSpec


@dataclass
class Lookup(TableNode):
    """
    LookupNode SQLNode
    """

    source_node: TableNode
    query_node_type = NodeType.LOOKUP

    @property
    def sql(self) -> Expression:
        return self.source_node.sql

    @classmethod
    def build(cls, context: SQLNodeContext) -> Lookup:

        input_sql_nodes = context.input_sql_nodes
        assert len(input_sql_nodes) == 1
        input_node = input_sql_nodes[0]

        if isinstance(input_node, TableNode):
            source_node = input_node
        else:
            assert isinstance(input_node, Project)
            source_node = input_node.table_node

        if context.sql_type == SQLType.AGGREGATION:
            columns_map = source_node.copy().columns_map
        else:
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
