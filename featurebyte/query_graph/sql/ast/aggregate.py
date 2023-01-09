"""
Module for aggregation related sql generation
"""
from __future__ import annotations

from typing import cast

from abc import abstractmethod
from dataclasses import dataclass

from sqlglot.expressions import Expression, Select

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import SQLType, quoted_identifier
from featurebyte.query_graph.sql.specs import AggregateAsAtSpec, LookupSpec


@dataclass  # type: ignore[misc]
class Aggregate(TableNode):
    """
    Aggregate SQLNode

    This node has two responsibilities:

    1. Serve as the source of aggregation (lookup, aggregate_asat, etc) when sql type is AGGREGATION
    2. Construct post-aggregation expressions when sql type is POST_AGGREGATION. This is a mapping
       from feature name(s) to internal aggregation name(s) which can be determined from the
       specific AggregationSpec instances.

    The heavy lifting of actual aggregation from the source is not done here but in the Aggregator.
    """

    source_node: TableNode

    @property
    def sql(self) -> Expression:
        return self.source_node.sql

    @classmethod
    def build(cls, context: SQLNodeContext) -> Aggregate:

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
            columns_map = cls.construct_columns_map(context=context, source_node=source_node)

        return Lookup(
            context=context,
            columns_map=columns_map,
            source_node=source_node,
        )

    @staticmethod
    @abstractmethod
    def construct_columns_map(
        context: SQLNodeContext, source_node: TableNode
    ) -> dict[str, Expression]:
        """
        Construct columns_map by instantiating the appropriate AggregationSpec

        Parameters
        ----------
        context: SQLNodeContext
            Context when building SQLNode
        source_node: TableNode
            The input TableNode to be aggregated

        Returns
        -------
        dict[str, Expression]
        """


@dataclass
class Lookup(Aggregate):
    """
    Lookup SQLNode
    """

    query_node_type = NodeType.LOOKUP

    @staticmethod
    def construct_columns_map(
        context: SQLNodeContext, source_node: TableNode
    ) -> dict[str, Expression]:
        # Create LookupSpec which determines the internal aggregated result names
        columns_map = {}
        specs = LookupSpec.from_lookup_query_node(
            context.query_node, source_expr=cast(Select, source_node.sql)
        )
        for spec in specs:
            columns_map[spec.feature_name] = quoted_identifier(spec.agg_result_name)
        return columns_map


@dataclass
class AsAt(Aggregate):
    """
    AsAt SQLNode
    """

    query_node_type = NodeType.AGGREGATE_AS_AT

    @staticmethod
    def construct_columns_map(
        context: SQLNodeContext, source_node: TableNode
    ) -> dict[str, Expression]:
        columns_map = {}
        spec = AggregateAsAtSpec.from_aggregate_asat_query_node(
            context.query_node, source_expr=cast(Select, source_node.sql)
        )
        feature_name = cast(str, spec.parameters.name)
        columns_map[feature_name] = quoted_identifier(spec.agg_result_name)
        return columns_map
