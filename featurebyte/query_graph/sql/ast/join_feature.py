"""
SQL generation for JOIN_FEATURE query node type
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, Tuple, cast

from sqlglot.expressions import Expression, Select, select

from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import ItemGroupbyParameters
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import (
    EventTableTimestampFilter,
    SQLType,
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.specs import AggregationSpec


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
        aggregated_table_expr, aggregation_specs = cls._join_intermediate_aggregation_result(
            graph=context.graph,
            feature_query_node=feature_query_node,
            view_node=view_node,
            view_entity_column=context.parameters["view_entity_column"],
            source_info=context.source_info,
            event_table_timestamp_filter=context.event_table_timestamp_filter,
        )

        # Apply any post-processing (e.g. filling missing value with 0 for count features)
        feature_expr = cls._get_feature_expr(
            graph=context.graph,
            feature_query_node=feature_query_node,
            source_info=context.source_info,
            aggregation_specs=aggregation_specs,
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
        source_info: SourceInfo,
        event_table_timestamp_filter: Optional[EventTableTimestampFilter] = None,
    ) -> Tuple[Select, dict[str, list[AggregationSpec]]]:
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
        source_info: SourceInfo
            Source info information
        event_table_timestamp_filter: Optional[EventTableTimestampFilter]
            Event table timestamp filter to apply if applicable

        Returns
        -------
        Select
        """

        from featurebyte.query_graph.sql.aggregator.item import (
            ItemAggregator,
        )
        from featurebyte.query_graph.sql.specs import (
            ItemAggregationSpec,
        )

        item_aggregator = ItemAggregator(
            source_info=source_info, to_inner_join_with_request_table=False
        )
        item_groupby_nodes = list(graph.iterate_nodes(feature_query_node, NodeType.ITEM_GROUPBY))

        agg_specs_mapping = defaultdict(list)
        for item_groupby_node in item_groupby_nodes:
            item_groupby_node_params = cast(ItemGroupbyParameters, item_groupby_node.parameters)

            # The EventView is conceptually the request table, so the entity column is the serving
            # column. Since it might not necessarily match the feature's serving name, we need to
            # use serving_names_mapping to perform this override.
            serving_names_mapping = {item_groupby_node_params.serving_names[0]: view_entity_column}

            agg_specs = ItemAggregationSpec.from_query_graph_node(
                node=item_groupby_node,
                graph=graph,
                source_info=source_info,
                serving_names_mapping=serving_names_mapping,
                event_table_timestamp_filter=event_table_timestamp_filter,
                agg_result_name_include_serving_names=True,
            )
            for agg_spec in agg_specs:
                item_aggregator.update(agg_spec)
                agg_specs_mapping[agg_spec.node_name].append(agg_spec)

        view_table_expr = select(*[
            get_qualified_column_identifier(col, "REQ") for col in view_node.columns
        ]).from_(cast(Select, view_node.sql).subquery(alias="REQ"))

        result = item_aggregator.update_aggregation_table_expr(
            table_expr=view_table_expr,
            point_in_time_column=SpecialColumnName.POINT_IN_TIME,
            current_columns=view_node.columns,
            current_query_index=0,
        )
        return result.updated_table_expr, dict(agg_specs_mapping)  # type: ignore[arg-type]

    @staticmethod
    def _get_feature_expr(
        graph: QueryGraphModel,
        feature_query_node: Node,
        source_info: SourceInfo,
        aggregation_specs: dict[str, list[AggregationSpec]],
    ) -> Expression:
        """
        Get the SQL expression for the feature

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph
        feature_query_node: Node
            Query node for the item aggregation feature
        source_info: SourceInfo
            Source information
        aggregation_specs: dict[str, list[AggregationSpec]]
            Aggregation specs to use when constructing SQLOperationGraph

        Returns
        -------
        Expression
        """

        from featurebyte.query_graph.sql.builder import (
            SQLOperationGraph,
        )

        sql_graph = SQLOperationGraph(
            graph,
            SQLType.POST_AGGREGATION,
            source_info=source_info,
            aggregation_specs=aggregation_specs,
        )
        sql_node = sql_graph.build(feature_query_node)
        return cast(Expression, sql_node.sql)
