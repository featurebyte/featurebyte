"""
SQL generation for JOIN_FEATURE query node type
"""
from __future__ import annotations

from typing import Optional, cast

from dataclasses import dataclass

from sqlglot import select
from sqlglot.expressions import Select

from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.generic import ItemGroupbyParameters
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import (
    SQLType,
    get_qualified_column_identifier,
    quoted_identifier,
)


@dataclass
class JoinFeature(TableNode):
    """
    JoinFeature SQLNode

    Responsible for generating SQL code for adding a Feature to an EventView
    """

    from_table_expr: Select
    query_node_type = NodeType.JOIN_FEATURE

    def from_query_impl(self, select_expr: Select) -> Select:
        return select_expr.from_(self.from_table_expr.subquery())

    @classmethod
    def build(cls, context: SQLNodeContext) -> Optional[JoinFeature]:

        from featurebyte.query_graph.sql.aggregator.item import ItemAggregator
        from featurebyte.query_graph.sql.builder import SQLOperationGraph
        from featurebyte.query_graph.sql.specs import ItemAggregationSpec

        view_entity_column = context.parameters["view_entity_column"]

        item_groupby_nodes = list(
            context.graph.iterate_nodes(context.query_node, NodeType.ITEM_GROUPBY)
        )
        item_aggregator = ItemAggregator(
            source_type=context.source_type, to_inner_join_with_request_table=False
        )
        for item_groupby_node in item_groupby_nodes:
            item_groupby_node_params = cast(ItemGroupbyParameters, item_groupby_node.parameters)
            serving_names_mapping = {item_groupby_node_params.serving_names[0]: view_entity_column}
            agg_specs = ItemAggregationSpec.from_query_graph_node(
                node=item_groupby_node,
                graph=context.graph,
                source_type=context.source_type,
                serving_names_mapping=serving_names_mapping,
            )
            for agg_spec in agg_specs:
                item_aggregator.update(agg_spec)

        view_node = cast(TableNode, context.input_sql_nodes[0])
        parameters = context.parameters
        view_table_expr = select(
            *[get_qualified_column_identifier(col, "REQ") for col in view_node.columns]
        ).from_(cast(Select, view_node.sql).subquery(alias="REQ"))
        joined_sql = item_aggregator.update_aggregation_table_expr(
            table_expr=view_table_expr,
            point_in_time_column=SpecialColumnName.POINT_IN_TIME,
            current_columns=view_node.columns,
            current_query_index=0,
        )

        sql_graph = SQLOperationGraph(
            context.graph, SQLType.POST_AGGREGATION, source_type=context.source_type
        )
        feature_query_node = context.input_sql_nodes[1].context.query_node
        sql_node = sql_graph.build(feature_query_node)
        feature_expr = sql_node.sql

        columns_map = {}
        for col in view_node.columns:
            columns_map[col] = quoted_identifier(col)
        columns_map[parameters["name"]] = feature_expr

        node = JoinFeature(
            context=context,
            columns_map=columns_map,
            from_table_expr=joined_sql.updated_table_expr,
        )
        return node
