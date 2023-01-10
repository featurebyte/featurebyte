"""
Module for groupby operation (non-time aware) sql generation
"""
from __future__ import annotations

from typing import cast

from dataclasses import dataclass

from sqlglot import expressions
from sqlglot.expressions import Expression, select

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import SQLType, quoted_identifier
from featurebyte.query_graph.sql.groupby_helper import get_aggregation_expression
from featurebyte.query_graph.sql.specs import ItemAggregationSpec


@dataclass
class ItemGroupby(TableNode):
    """
    ItemGroupby SQLNode
    """

    input_node: TableNode
    keys: list[str]
    agg_expr: Expression
    output_name: str
    query_node_type = NodeType.ITEM_GROUPBY

    @property
    def sql(self) -> Expression:
        quoted_keys = [quoted_identifier(k) for k in self.keys]
        expr = (
            select(
                *quoted_keys, expressions.alias_(self.agg_expr, quoted_identifier(self.output_name))
            )
            .from_(self.input_node.sql_nested())
            .group_by(*quoted_keys)
        )
        return expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> ItemGroupby | None:
        if context.sql_type == SQLType.POST_AGGREGATION:
            return None
        parameters = context.parameters
        agg_expr = get_aggregation_expression(
            agg_func=parameters["agg_func"], input_column=parameters["parent"]
        )
        columns_map = {}
        for key in parameters["keys"]:
            columns_map[key] = quoted_identifier(key)
        output_name = parameters["name"]
        columns_map[output_name] = quoted_identifier(output_name)
        node = ItemGroupby(
            context=context,
            columns_map=columns_map,
            input_node=cast(TableNode, context.input_sql_nodes[0]),
            keys=parameters["keys"],
            agg_expr=agg_expr,
            output_name=output_name,
        )
        return node


@dataclass
class AggregatedItemGroupby(TableNode):
    """Node with ItemData already aggregated

    The purpose of this node is to allow feature SQL generation to retrieve the post-aggregation
    feature transform expression. The columns_map of this node has the mapping from user defined
    feature names to internal aggregated column names. The feature expression can be obtained by
    calling get_column_expr().
    """

    query_node_type = NodeType.ITEM_GROUPBY

    @property
    def sql(self) -> Expression:
        # This will not be called anywhere
        raise NotImplementedError()

    @classmethod
    def build(cls, context: SQLNodeContext) -> AggregatedItemGroupby | None:
        sql_node = None
        if context.sql_type == SQLType.POST_AGGREGATION:
            agg_spec = ItemAggregationSpec.from_item_groupby_query_node(context.query_node)
            columns_map = {agg_spec.feature_name: quoted_identifier(agg_spec.agg_result_name)}
            sql_node = AggregatedItemGroupby(context=context, columns_map=columns_map)
        return sql_node
