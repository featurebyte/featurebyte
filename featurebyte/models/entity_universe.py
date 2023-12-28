"""
This module contains the logic to construct the entity universe for a given node
"""
from __future__ import annotations

from typing import List, Optional, Tuple, cast

from abc import abstractmethod
from datetime import datetime

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, select

from featurebyte.enum import SourceType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.offline_store_ingest_query import OfflineStoreIngestQueryGraph
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import AggregateAsAtNode, ItemGroupbyNode, LookupNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType, quoted_identifier
from featurebyte.query_graph.sql.template import SqlExpressionTemplate
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer

CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER = "__fb_current_feature_timestamp"
LAST_MATERIALIZED_TIMESTAMP_PLACEHOLDER = "__fb_last_materialized_timestamp"


class BaseEntityUniverseConstructor:
    """
    Base class for entity universe constructor.

    Prepares the context of what is commonly required when determining the entity universe for a
    given aggregation node: the node parameters the SQL expression for the input of the aggregation
    node.
    """

    def __init__(self, graph: QueryGraphModel, node: Node, source_type: SourceType):
        flat_graph, node_name_map = GraphFlatteningTransformer(graph=graph).transform()
        flat_node = flat_graph.get_node_by_name(node_name_map[node.name])
        self.graph = flat_graph
        self.node = flat_node

        sql_graph = SQLOperationGraph(self.graph, SQLType.AGGREGATION, source_type=source_type)
        sql_node = sql_graph.build(self.node)
        self.aggregate_input_expr = sql_node.sql

    @abstractmethod
    def get_entity_universe_template(self) -> Expression:
        """
        Returns a SQL expression for the universe of the entity with placeholders for current
        feature timestamp and last materialization timestamp
        """


class LookupNodeEntityUniverseConstructor(BaseEntityUniverseConstructor):
    """
    Construct the entity universe expression for lookup node
    """

    def get_entity_universe_template(self) -> Expression:
        node = cast(LookupNode, self.node)

        if node.parameters.scd_parameters is not None:
            ts_col = node.parameters.scd_parameters.effective_timestamp_column
        elif node.parameters.event_parameters is not None:
            ts_col = node.parameters.event_parameters.event_timestamp_column
        else:
            ts_col = None

        if ts_col:
            aggregate_input_expr = self.aggregate_input_expr.where(
                expressions.and_(
                    expressions.GTE(
                        this=quoted_identifier(ts_col),
                        expression=LAST_MATERIALIZED_TIMESTAMP_PLACEHOLDER,
                    ),
                    expressions.LT(
                        this=quoted_identifier(ts_col),
                        expression=CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER,
                    ),
                )
            )
        else:
            aggregate_input_expr = self.aggregate_input_expr

        universe_expr = (
            select(
                expressions.alias_(
                    quoted_identifier(node.parameters.entity_column),
                    alias=node.parameters.serving_name,
                    quoted=True,
                )
            )
            .distinct()
            .from_(aggregate_input_expr.subquery())
        )
        return universe_expr


class AggregateAsAtNodeEntityUniverseConstructor(BaseEntityUniverseConstructor):
    """
    Construct the entity universe expression for aggregate as at node
    """

    def get_entity_universe_template(self) -> Expression:
        node = cast(AggregateAsAtNode, self.node)

        ts_col = node.parameters.effective_timestamp_column
        filtered_aggregate_input_expr = self.aggregate_input_expr.where(
            expressions.and_(
                expressions.GTE(
                    this=quoted_identifier(ts_col),
                    expression=LAST_MATERIALIZED_TIMESTAMP_PLACEHOLDER,
                ),
                expressions.LT(
                    this=quoted_identifier(ts_col), expression=CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER
                ),
            )
        )
        universe_expr = (
            select(
                *[
                    expressions.alias_(quoted_identifier(key), alias=serving_name, quoted=True)
                    for key, serving_name in zip(
                        node.parameters.keys, node.parameters.serving_names
                    )
                ]
            )
            .distinct()
            .from_(filtered_aggregate_input_expr.subquery())
        )
        return universe_expr


class ItemAggregateNodeEntityUniverseConstructor(BaseEntityUniverseConstructor):
    """
    Construct the entity universe expression for item aggregate node
    """

    def get_entity_universe_template(self) -> Expression:
        node = cast(ItemGroupbyNode, self.node)

        ts_col = node.parameters.event_timestamp_column_name
        filtered_aggregate_input_expr = self.aggregate_input_expr.where(
            expressions.and_(
                expressions.GTE(
                    this=quoted_identifier(ts_col),
                    expression=LAST_MATERIALIZED_TIMESTAMP_PLACEHOLDER,
                ),
                expressions.LT(
                    this=quoted_identifier(ts_col), expression=CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER
                ),
            )
        )
        universe_expr = (
            select(
                *[
                    expressions.alias_(quoted_identifier(key), alias=serving_name, quoted=True)
                    for key, serving_name in zip(
                        node.parameters.keys, node.parameters.serving_names
                    )
                ]
            )
            .distinct()
            .from_(filtered_aggregate_input_expr.subquery())
        )
        return universe_expr


def get_entity_universe_constructor(
    graph: QueryGraphModel, node: Node, source_type: SourceType
) -> BaseEntityUniverseConstructor:
    """
    Returns the entity universe constructor for the given node

    Parameters
    ----------
    graph: QueryGraphModel
        The query graph
    node: Node
        The node for which the entity universe constructor is to be returned
    source_type: SourceType
        Source type information

    Returns
    -------
    BaseEntityUniverseConstructor

    Raises
    ------
    NotImplementedError
        If the node type is not supported
    """
    node_type_to_constructor = {
        NodeType.LOOKUP: LookupNodeEntityUniverseConstructor,
        NodeType.AGGREGATE_AS_AT: AggregateAsAtNodeEntityUniverseConstructor,
        NodeType.ITEM_GROUPBY: ItemAggregateNodeEntityUniverseConstructor,
    }
    if node.type in node_type_to_constructor:
        return node_type_to_constructor[node.type](graph, node, source_type)
    raise NotImplementedError(f"Unsupported node type: {node.type}")


def get_combined_universe(
    graph_and_node_pairs: List[Tuple[QueryGraphModel, Node]], source_type: SourceType
) -> Expression:
    """
    Returns the combined entity universe expression

    Parameters
    ----------
    graph_and_node_pairs: List[Tuple[QueryGraphModel, Node]]
        List of (graph, node) pairs for which the entity universe is to be constructed
    source_type: SourceType
        Source type information

    Returns
    -------
    Expression
    """
    combined_universe_expr: Optional[Expression] = None
    processed_universe_exprs = set()

    for graph, node in graph_and_node_pairs:
        entity_universe_constructor = get_entity_universe_constructor(graph, node, source_type)
        current_universe_expr = entity_universe_constructor.get_entity_universe_template()
        if combined_universe_expr is None:
            combined_universe_expr = current_universe_expr
        elif current_universe_expr not in processed_universe_exprs:
            combined_universe_expr = expressions.Union(
                this=current_universe_expr,
                distinct=True,
                expression=combined_universe_expr,
            )
        processed_universe_exprs.add(current_universe_expr)

    assert combined_universe_expr is not None
    return combined_universe_expr


def construct_window_aggregates_universe(
    serving_names: List[str],
    aggregate_result_table_names: List[str],
) -> Expression:
    """
    Construct the entity universe expression for window aggregate

    Parameters
    ----------
    serving_names: List[str]
        The serving names of the entities
    aggregate_result_table_names: List[str]
        The names of the aggregate result tables

    Returns
    -------
    Expression
    """
    if not serving_names:
        return expressions.select(
            expressions.alias_(make_literal_value(1), "dummy_entity", quoted=True)
        )

    assert len(aggregate_result_table_names) > 0

    # select distinct serving names across all aggregation result tables
    distinct_serving_names_from_tables = [
        select(*[quoted_identifier(serving_name) for serving_name in serving_names])
        .distinct()
        .from_(table_name)
        for table_name in aggregate_result_table_names
    ]

    union_expr = distinct_serving_names_from_tables[0]
    for expr in distinct_serving_names_from_tables[1:]:
        union_expr = expressions.Union(this=expr, distinct=True, expression=union_expr)  # type: ignore

    return union_expr


class EntityUniverseModel(FeatureByteBaseModel):
    """
    EntityUniverseModel class
    """

    # query_template is a SQL expression template with placeholders __fb_current_feature_timestamp,
    # __fb_last_materialized_timestamp which will be replaced with actual values when entity
    # universe needs to be generated.
    query_template: SqlglotExpressionModel

    @classmethod
    def create(
        cls,
        serving_names: List[str],
        aggregate_result_table_names: List[str],
        offline_ingest_graphs: List[OfflineStoreIngestQueryGraph],
        source_type: SourceType,
    ) -> EntityUniverseModel:
        """
        Create a new EntityUniverseModel object

        Parameters
        ----------
        serving_names: List[str]
            The serving names of the entities
        aggregate_result_table_names: List[str]
            The names of the aggregate result tables for window aggregates
        offline_ingest_graphs: List[OfflineStoreIngestQueryGraph]
            The offline ingest graphs that the entity universe is to be constructed from
        source_type: SourceType
            Source type information

        Returns
        -------
        EntityUniverse
        """
        if aggregate_result_table_names:
            universe_expr = construct_window_aggregates_universe(
                serving_names=serving_names,
                aggregate_result_table_names=aggregate_result_table_names,
            )
        else:
            graph_and_node_pairs = []
            for offline_ingest_graph in offline_ingest_graphs:
                for info in offline_ingest_graph.aggregation_nodes_info:
                    node = offline_ingest_graph.graph.get_node_by_name(info.node_name)
                    graph_and_node_pairs.append((offline_ingest_graph.graph, node))
            universe_expr = get_combined_universe(graph_and_node_pairs, source_type)

        return EntityUniverseModel(query_template=SqlglotExpressionModel.create(universe_expr))

    def get_entity_universe_expr(
        self,
        current_feature_timestamp: datetime,
        last_materialized_timestamp: Optional[datetime],
    ) -> Select:
        """
        Get a concrete SQL expression for the entity universe for the given feature timestamp and
        optionally the last materialized timestamp.

        Parameters
        ----------
        current_feature_timestamp : datetime
            Current feature timestamp
        last_materialized_timestamp : Optional[datetime]
            Last materialized timestamp

        Returns
        -------
        Expression
        """
        params = {
            CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER: make_literal_value(
                current_feature_timestamp, cast_as_timestamp=True
            ),
        }
        if last_materialized_timestamp is not None:
            params[LAST_MATERIALIZED_TIMESTAMP_PLACEHOLDER] = make_literal_value(
                last_materialized_timestamp, cast_as_timestamp=True
            )
        else:
            params[LAST_MATERIALIZED_TIMESTAMP_PLACEHOLDER] = make_literal_value(
                "1970-01-01 00:00:00", cast_as_timestamp=True
            )
        return cast(
            Select,
            SqlExpressionTemplate(self.query_template.expr).render(data=params, as_str=False),
        )
