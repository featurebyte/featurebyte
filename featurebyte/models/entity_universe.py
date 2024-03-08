"""
This module contains the logic to construct the entity universe for a given node
"""
from __future__ import annotations

from typing import List, Optional, cast

from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, Subqueryable, select

from featurebyte.enum import SourceType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.parent_serving import EntityLookupStep
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import AggregateAsAtNode, ItemGroupbyNode, LookupNode
from featurebyte.query_graph.node.input import EventTableInputNodeParameters
from featurebyte.query_graph.node.nested import ItemViewGraphNodeParameters
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import (
    EventTableTimestampFilter,
    SQLType,
    get_fully_qualified_table_name,
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.template import SqlExpressionTemplate
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer

CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER = "__fb_current_feature_timestamp"
LAST_MATERIALIZED_TIMESTAMP_PLACEHOLDER = "__fb_last_materialized_timestamp"


@dataclass
class EntityUniverseParams:
    """
    Parameters for each entity universe to be constructed
    """

    graph: QueryGraphModel
    node: Node
    join_steps: Optional[List[EntityLookupStep]]


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

        sql_graph = SQLOperationGraph(
            self.graph,
            SQLType.AGGREGATION,
            source_type=source_type,
            event_table_timestamp_filter=self.get_event_table_timestamp_filter(
                graph=graph,
                node=node,
            ),
        )
        sql_node = sql_graph.build(self.node)
        self.aggregate_input_expr = sql_node.sql

    @abstractmethod
    def get_entity_universe_template(self) -> Expression:
        """
        Returns a SQL expression for the universe of the entity with placeholders for current
        feature timestamp and last materialization timestamp
        """

    @abstractmethod
    def get_serving_names(self) -> List[str]:
        """
        Return list of serving names
        """

    @classmethod
    def get_event_table_timestamp_filter(  # pylint: disable=useless-return
        cls, graph: QueryGraphModel, node: Node
    ) -> Optional[EventTableTimestampFilter]:
        """
        Construct an instance of EventTableTimestampFilter used to filter input EventTable when
        applicable. To be passed to SQLOperationGraph when constructing aggregate input expression

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph before flattening
        node: Node
            Node corresponding to the aggregation node

        Returns
        -------
        Optional[EventTableTimestampFilter]
        """
        _ = graph
        _ = node
        return None


class LookupNodeEntityUniverseConstructor(BaseEntityUniverseConstructor):
    """
    Construct the entity universe expression for lookup node
    """

    def get_serving_names(self) -> List[str]:
        node = cast(LookupNode, self.node)
        return [node.parameters.serving_name]

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

    def get_serving_names(self) -> List[str]:
        node = cast(AggregateAsAtNode, self.node)
        return node.parameters.serving_names

    def get_entity_universe_template(self) -> Expression:
        node = cast(AggregateAsAtNode, self.node)

        if not node.parameters.serving_names:
            return get_dummy_entity_universe()

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

    def get_serving_names(self) -> List[str]:
        node = cast(ItemGroupbyNode, self.node)
        return node.parameters.serving_names

    @classmethod
    def get_event_table_timestamp_filter(
        cls, graph: QueryGraphModel, node: Node
    ) -> Optional[EventTableTimestampFilter]:
        # Find the graph node corresponding to the ItemView. From that graph node's parameters we
        # can get the EventTable's id corresponding to this ItemView.
        graph_node = None
        event_table_id = None
        for graph_node in graph.iterate_nodes(node, NodeType.GRAPH):
            if isinstance(graph_node.parameters, ItemViewGraphNodeParameters):
                event_table_id = graph_node.parameters.metadata.event_table_id
                break
        assert graph_node is not None
        assert event_table_id is not None

        # Get the EventTable's event timestamp column
        event_timestamp_column = None
        for input_node in graph.iterate_nodes(graph_node, NodeType.INPUT):
            if (
                isinstance(input_node.parameters, EventTableInputNodeParameters)
                and input_node.parameters.id == event_table_id
            ):
                event_timestamp_column = input_node.parameters.timestamp_column
                break
        assert event_timestamp_column is not None

        # Construct a filter to be applied to the EventTable
        event_table_timestamp_filter = EventTableTimestampFilter(
            timestamp_column_name=event_timestamp_column,
            event_table_id=event_table_id,
            start_timestamp_placeholder_name=LAST_MATERIALIZED_TIMESTAMP_PLACEHOLDER,
            end_timestamp_placeholder_name=CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER,
            to_cast_placeholders=False,
        )
        return event_table_timestamp_filter

    def get_entity_universe_template(self) -> Expression:
        node = cast(ItemGroupbyNode, self.node)
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
            .from_(self.aggregate_input_expr.subquery())
        )
        return universe_expr


def get_dummy_entity_universe() -> Select:
    """
    Returns a dummy entity universe (actual value not important since it doesn't affect features
    calculation)

    Returns
    -------
    Select
    """
    return expressions.select(
        expressions.alias_(make_literal_value(1), "dummy_entity", quoted=True)
    )


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
        return node_type_to_constructor[node.type](graph, node, source_type)  # type: ignore
    raise NotImplementedError(f"Unsupported node type: {node.type}")


def _apply_join_step(universe_expr: Expression, join_step: EntityLookupStep) -> Expression:
    assert isinstance(universe_expr, Subqueryable)
    table_details_dict = join_step.table.tabular_source.table_details.dict()
    updated_universe_expr = (
        select(
            expressions.alias_(
                get_qualified_column_identifier(join_step.child.key, "CHILD"),
                alias=join_step.child.serving_name,
                quoted=True,
            )
        )
        .from_(universe_expr.subquery(alias="PARENT"))
        .join(
            get_fully_qualified_table_name(table_details_dict),
            join_alias="CHILD",
            join_type="LEFT",
            on=expressions.EQ(
                this=get_qualified_column_identifier(join_step.parent.serving_name, "PARENT"),
                expression=get_qualified_column_identifier(join_step.parent.key, "CHILD"),
            ),
        )
        .distinct()
    )
    return updated_universe_expr


def apply_join_steps(universe_expr: Expression, join_steps: List[EntityLookupStep]) -> Expression:
    """
    Apply join steps to lookup child entities from parent entities

    Note that this is the inverse of parent entity lookup - the entity universe is based on the
    primary entity, but the aggregation node's entity could be non-primary entity. This is a
    one-to-many lookup.

    Parameters
    ----------
    universe_expr: Expression
        Entity universe query in non-primary entity
    join_steps: List[EntityLookupStep]
        A series of join steps that convert the entity universe to be in terms of primary entity

    Returns
    -------
    Expression
    """
    for join_step in join_steps:
        universe_expr = _apply_join_step(universe_expr, join_step)
    return universe_expr


def get_combined_universe(
    entity_universe_params: List[EntityUniverseParams],
    source_type: SourceType,
) -> Expression:
    """
    Returns the combined entity universe expression

    Parameters
    ----------
    entity_universe_params: List[EntityUniverseParams]
        Parameters of the entity universe to be constructed
    source_type: SourceType
        Source type information

    Returns
    -------
    Expression
    """
    combined_universe_expr: Optional[Expression] = None
    processed_universe_exprs = set()

    for params in entity_universe_params:
        entity_universe_constructor = get_entity_universe_constructor(
            params.graph, params.node, source_type
        )
        current_universe_expr = entity_universe_constructor.get_entity_universe_template()
        if params.join_steps:
            current_universe_expr = apply_join_steps(current_universe_expr, params.join_steps[::-1])
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
        return get_dummy_entity_universe()

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
