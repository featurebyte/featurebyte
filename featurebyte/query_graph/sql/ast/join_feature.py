"""
SQL generation for JOIN_FEATURE query node type
"""
from __future__ import annotations

from typing import Optional, cast

from dataclasses import dataclass

from sqlglot.expressions import Expression, Select, select

from featurebyte.enum import SourceType, SpecialColumnName
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
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

    aggregated_table_expr: Select
    query_node_type = NodeType.JOIN_FEATURE

    def from_query_impl(self, select_expr: Select) -> Select:
        return select_expr.from_(self.aggregated_table_expr.subquery())

    @classmethod
    def build(cls, context: SQLNodeContext) -> Optional[JoinFeature]:
        feature_query_node = context.input_sql_nodes[1].context.query_node
        view_node = cast(TableNode, context.input_sql_nodes[0])

        # Perform item aggregation and join the result with the EventView
        aggregated_table_expr = cls._join_intermediate_aggregation_result(
            graph=context.graph,
            feature_query_node=feature_query_node,
            view_node=view_node,
            view_entity_column=context.parameters["view_entity_column"],
            source_type=context.source_type,
        )

        # Apply any post-processing (e.g. filling missing value with 0 for count features)
        feature_expr = cls._get_feature_expr(
            graph=context.graph,
            feature_query_node=feature_query_node,
            source_type=context.source_type,
        )

        columns_map = {}
        for col in view_node.columns:
            columns_map[col] = quoted_identifier(col)
        columns_map[context.parameters["name"]] = feature_expr

        node = JoinFeature(
            context=context,
            columns_map=columns_map,
            aggregated_table_expr=aggregated_table_expr,
        )
        return node

    @staticmethod
    def _join_intermediate_aggregation_result(
        graph: QueryGraphModel,
        feature_query_node: Node,
        view_node: TableNode,
        view_entity_column: str,
        source_type: SourceType,
    ) -> Select:
        """
        Join the intermediate aggregation result of item aggregation with the EventView

        The behaviour is as if we are serving the non-time based item aggregation feature using the
        EventView as the request table.

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph
        feature_query_node: Node
            Query node for the item aggregation feature
        view_node: TableNode
            A TableNode representing the EventView
        view_entity_column: str
            The column in the EventView to be used as the entity column when joining the
            intermediate aggregation result
        source_type: SourceType
            Source type information

        Returns
        -------
        Select
        """

        from featurebyte.query_graph.sql.aggregator.item import (  # pylint: disable=import-outside-toplevel
            ItemAggregator,
        )
        from featurebyte.query_graph.sql.specs import (  # pylint: disable=import-outside-toplevel
            ItemAggregationSpec,
        )

        item_aggregator = ItemAggregator(
            source_type=source_type, to_inner_join_with_request_table=False
        )
        item_groupby_nodes = list(graph.iterate_nodes(feature_query_node, NodeType.ITEM_GROUPBY))

        for item_groupby_node in item_groupby_nodes:
            item_groupby_node_params = cast(ItemGroupbyParameters, item_groupby_node.parameters)

            # The EventView is conceptually the request table, so the entity column is the serving
            # column. Since it might not necessarily match the feature's serving name, we need to
            # use serving_names_mapping to perform this override.
            serving_names_mapping = {item_groupby_node_params.serving_names[0]: view_entity_column}

            agg_specs = ItemAggregationSpec.from_query_graph_node(
                node=item_groupby_node,
                graph=graph,
                source_type=source_type,
                serving_names_mapping=serving_names_mapping,
            )
            for agg_spec in agg_specs:
                item_aggregator.update(agg_spec)

        view_table_expr = select(
            *[get_qualified_column_identifier(col, "REQ") for col in view_node.columns]
        ).from_(cast(Select, view_node.sql).subquery(alias="REQ"))

        result = item_aggregator.update_aggregation_table_expr(
            table_expr=view_table_expr,
            point_in_time_column=SpecialColumnName.POINT_IN_TIME,
            current_columns=view_node.columns,
            current_query_index=0,
        )
        return result.updated_table_expr

    @staticmethod
    def _get_feature_expr(
        graph: QueryGraphModel,
        feature_query_node: Node,
        source_type: SourceType,
    ) -> Expression:
        """
        Get the SQL expression for the feature

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph
        feature_query_node: Node
            Query node for the item aggregation feature
        source_type: SourceType
            Source type information

        Returns
        -------
        Expression
        """

        from featurebyte.query_graph.sql.builder import (  # pylint: disable=import-outside-toplevel
            SQLOperationGraph,
        )

        sql_graph = SQLOperationGraph(graph, SQLType.POST_AGGREGATION, source_type=source_type)
        sql_node = sql_graph.build(feature_query_node)
        return cast(Expression, sql_node.sql)
