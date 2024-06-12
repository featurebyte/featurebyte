"""
This module contains the logic to construct the entity universe for a given node
"""

from __future__ import annotations

from typing import List, Optional, Sequence, cast

from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, Subqueryable, select

from featurebyte.common.model_util import parse_duration_string
from featurebyte.enum import InternalName, SourceType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.item_table import ItemTableModel
from featurebyte.models.parent_serving import EntityLookupStep
from featurebyte.models.proxy_table import TableModel
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    AggregateAsAtNode,
    GroupByNode,
    ItemGroupbyNode,
    LookupNode,
)
from featurebyte.query_graph.node.input import EventTableInputNodeParameters
from featurebyte.query_graph.node.nested import ItemViewGraphNodeParameters
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import (
    EventTableTimestampFilter,
    SQLType,
    get_fully_qualified_table_name,
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.online_serving_util import get_online_store_table_name
from featurebyte.query_graph.sql.specs import AggregationType, TileBasedAggregationSpec
from featurebyte.query_graph.sql.template import SqlExpressionTemplate
from featurebyte.query_graph.sql.tile_util import calculate_last_tile_index_expr
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer

CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER = "__fb_current_feature_timestamp"
LAST_MATERIALIZED_TIMESTAMP_PLACEHOLDER = "__fb_last_materialized_timestamp"


def columns_not_null(columns: Sequence[str]) -> Expression:
    """
    Returns an expression for a boolean condition that evaluates to true if none of the columns are
    null. To be used to filter out rows with missing entity values in the entity universe.

    Parameters
    ----------
    columns: List[str]
        List of column names to check

    Returns
    -------
    Expression
    """
    return expressions.and_(
        *[
            expressions.Is(
                this=quoted_identifier(column),
                expression=expressions.Not(this=expressions.Null()),
            )
            for column in columns
        ]
    )


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


DUMMY_ENTITY_UNIVERSE = get_dummy_entity_universe()


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
        self.adapter = get_sql_adapter(source_type)

    @abstractmethod
    def get_entity_universe_template(self) -> List[Expression]:
        """
        Returns SQL expressions for the universe of the entity with placeholders for current
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

    def get_entity_universe_template(self) -> List[Expression]:
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
            .where(columns_not_null([node.parameters.entity_column]))
        )
        return [universe_expr]


class AggregateAsAtNodeEntityUniverseConstructor(BaseEntityUniverseConstructor):
    """
    Construct the entity universe expression for aggregate as at node
    """

    def get_serving_names(self) -> List[str]:
        node = cast(AggregateAsAtNode, self.node)
        return node.parameters.serving_names

    def get_entity_universe_template(self) -> List[Expression]:
        node = cast(AggregateAsAtNode, self.node)

        if not node.parameters.serving_names:
            return [DUMMY_ENTITY_UNIVERSE]

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
            .where(columns_not_null(node.parameters.keys))
        )
        return [universe_expr]


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

    def get_entity_universe_template(self) -> List[Expression]:
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
            .where(columns_not_null(node.parameters.keys))
        )
        return [universe_expr]


class TileBasedAggregateNodeEntityUniverseConstructor(BaseEntityUniverseConstructor):
    """
    Construct the entity universe expression for tile based aggregate node
    """

    def get_serving_names(self) -> List[str]:
        node = cast(GroupByNode, self.node)
        return node.parameters.serving_names

    def get_entity_universe_template(self) -> List[Expression]:
        node = cast(GroupByNode, self.node)

        if not node.parameters.serving_names:
            return [DUMMY_ENTITY_UNIVERSE]

        agg_specs = TileBasedAggregationSpec.from_groupby_query_node(
            graph=self.graph,
            groupby_node=node,
            adapter=self.adapter,
            agg_result_name_include_serving_names=True,
        )
        out = []
        for agg_spec in agg_specs:
            if agg_spec.aggregation_type == AggregationType.WINDOW:
                universe = self._get_universe_expr_from_internal_online_store(node, agg_spec)
            else:
                universe = self._get_entity_universe_from_source(node)
            out.append(universe)
        return out

    def _get_universe_expr_from_internal_online_store(
        self, node: GroupByNode, agg_spec: TileBasedAggregationSpec
    ) -> Expression:
        result_type = self.adapter.get_physical_type_from_dtype(agg_spec.dtype)
        online_store_table_name = get_online_store_table_name(
            set(agg_spec.entity_ids if agg_spec.entity_ids is not None else []),
            result_type=result_type,
        )
        online_store_table_condition = expressions.EQ(
            this=quoted_identifier(InternalName.ONLINE_STORE_RESULT_NAME_COLUMN),
            expression=make_literal_value(agg_spec.agg_result_name),
        )
        universe_expr = (
            select(
                *[quoted_identifier(serving_name) for serving_name in node.parameters.serving_names]
            )
            .distinct()
            .from_(expressions.Table(this=online_store_table_name))
            .where(
                expressions.and_(
                    online_store_table_condition, columns_not_null(node.parameters.serving_names)
                )
            )
        )
        return universe_expr

    def _get_entity_universe_from_source(self, node: GroupByNode) -> Expression:
        ts_col = node.parameters.timestamp
        last_tile_index_expr = calculate_last_tile_index_expr(
            adapter=self.adapter,
            point_in_time_expr=quoted_identifier(CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER),
            frequency=node.parameters.feature_job_setting.period_seconds,
            time_modulo_frequency=node.parameters.feature_job_setting.offset_seconds,
            offset=(
                parse_duration_string(node.parameters.offset) if node.parameters.offset else None
            ),
        )
        last_tile_index_timestamp = expressions.Anonymous(
            this="F_INDEX_TO_TIMESTAMP",
            expressions=[
                last_tile_index_expr,
                make_literal_value(node.parameters.feature_job_setting.offset_seconds),
                make_literal_value(node.parameters.feature_job_setting.blind_spot_seconds),
                make_literal_value(node.parameters.feature_job_setting.period_seconds // 60),
            ],
        )
        filtered_aggregate_input_expr = self.aggregate_input_expr.where(
            expressions.and_(
                expressions.GTE(
                    this=quoted_identifier(ts_col),
                    expression=LAST_MATERIALIZED_TIMESTAMP_PLACEHOLDER,
                ),
                expressions.LT(
                    this=quoted_identifier(ts_col), expression=last_tile_index_timestamp
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
            .where(columns_not_null(node.parameters.keys))
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
        NodeType.GROUPBY: TileBasedAggregateNodeEntityUniverseConstructor,
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
) -> Optional[Expression]:
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
    Optional[Expression]
    """
    combined_universe_expr: Optional[Expression] = None
    processed_universe_exprs = set()
    has_dummy_entity_universe = False

    for params in entity_universe_params:
        entity_universe_constructor = get_entity_universe_constructor(
            params.graph, params.node, source_type
        )
        for current_universe_expr in entity_universe_constructor.get_entity_universe_template():
            if current_universe_expr == DUMMY_ENTITY_UNIVERSE:
                # Add dummy entity universe later after going through all other universes
                has_dummy_entity_universe = True
                continue
            if params.join_steps:
                current_universe_expr = apply_join_steps(
                    current_universe_expr, params.join_steps[::-1]
                )
            if combined_universe_expr is None:
                combined_universe_expr = current_universe_expr
            elif current_universe_expr not in processed_universe_exprs:
                combined_universe_expr = expressions.Union(
                    this=current_universe_expr,
                    distinct=True,
                    expression=combined_universe_expr,
                )
            processed_universe_exprs.add(current_universe_expr)

    if has_dummy_entity_universe and combined_universe_expr is None:
        # Construct dummy entity universe only when there is no other universes to union with. This
        # is to handle the case when a feature is made up of window aggregates with and without
        # entity (such ingest graph is not decomposed). When that happens, the dummy universe is
        # ignored.
        combined_universe_expr = DUMMY_ENTITY_UNIVERSE

    return combined_universe_expr


def get_item_relation_table_lookup_universe(item_table_model: TableModel) -> expressions.Select:
    """
    Get the entity universe for a relation table that is an ItemTable. This is used when looking up
    a parent entity using a child entity (item id column) in an ItemTable.

    Parameters
    ----------
    item_table_model: TableModel
        Item table model

    Returns
    -------
    expressions.Select
    """
    assert isinstance(item_table_model, ItemTableModel)
    event_table_model = item_table_model.event_table_model
    assert event_table_model is not None
    filtered_event_table_expr = (
        expressions.select(quoted_identifier(event_table_model.event_id_column))
        .from_(
            get_fully_qualified_table_name((event_table_model.tabular_source.table_details.dict()))
        )
        .where(
            expressions.and_(
                expressions.GTE(
                    this=quoted_identifier(event_table_model.event_timestamp_column),
                    expression=LAST_MATERIALIZED_TIMESTAMP_PLACEHOLDER,
                ),
                expressions.LT(
                    this=quoted_identifier(event_table_model.event_timestamp_column),
                    expression=CURRENT_FEATURE_TIMESTAMP_PLACEHOLDER,
                ),
            )
        )
    )
    universe = (
        expressions.select(quoted_identifier(item_table_model.item_id_column))
        .distinct()
        .from_(
            expressions.Table(
                this=get_fully_qualified_table_name(
                    item_table_model.tabular_source.table_details.dict()
                ),
                alias="ITEM",
            ),
        )
        .join(
            filtered_event_table_expr.subquery(alias="EVENT"),
            on=expressions.EQ(
                this=get_qualified_column_identifier(item_table_model.event_id_column, "ITEM"),
                expression=get_qualified_column_identifier(
                    event_table_model.event_id_column, "EVENT"
                ),
            ),
            join_type="INNER",
        )
    )
    return universe


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
