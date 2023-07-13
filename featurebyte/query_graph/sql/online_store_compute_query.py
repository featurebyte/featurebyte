"""
SQL generation for online store compute queries
"""
from __future__ import annotations

from typing import List, Optional, Tuple, cast

from dataclasses import dataclass

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression, alias_, select

from featurebyte.enum import InternalName, SourceType, SpecialColumnName
from featurebyte.models.online_store_compute_query import OnlineStoreComputeQueryModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.adapter import BaseAdapter, get_sql_adapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME, quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.sql.online_serving_util import get_online_store_table_name
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec
from featurebyte.query_graph.sql.tile_util import calculate_first_and_last_tile_indices


@dataclass
class OnlineStoreUniverse:
    """
    Represents a query that produces the "universe" of online store - the unique set of entities for
    a simple feature (derived from a single tile table) as at a point in time.

    It is derived based on the feature (e.g. a feature with larger window size will have a larger
    set of universe) and the underlying data (whether there are any events that occurred within the
    feature window as at a particular point in time).
    """

    expr: expressions.Select
    columns: List[str]


@dataclass
class PrecomputeQueryParams:
    """
    Information required to generate a OnlineStoreComputeQueryModel

    agg_spec: TileBasedAggregationSpec
        Aggregation specification
    universe: OnlineStoreUniverse
        Query that produces the universe entities for the aggregation
    """

    agg_spec: TileBasedAggregationSpec
    universe: OnlineStoreUniverse


class OnlineStorePrecomputePlan:
    """
    OnlineStorePrecomputePlan is responsible for extracting universe for online store feature
    pre-computation

    Parameters
    ----------
    graph: QueryGraph
        Query graph
    node: Node
        Query graph node
    adapter: BaseAdapter
        Instance of BaseAdapter for engine specific sql generation
    """

    def __init__(self, graph: QueryGraph, node: Node, adapter: BaseAdapter):
        self.adapter = adapter
        self.params_by_agg_result_name: dict[str, PrecomputeQueryParams] = {}
        self._update(graph, node)

    def construct_online_store_precompute_queries(
        self, source_type: SourceType
    ) -> list[OnlineStoreComputeQueryModel]:
        """
        Construct SQL queries for online store pre-computation

        Parameters
        ----------
        source_type: SourceType
            Source type information

        Returns
        -------
        list[OnlineStoreComputeQueryModel]
        """
        result = []
        for _, agg_params in self.params_by_agg_result_name.items():
            query = self._construct_online_store_precompute_query(
                params=agg_params,
                source_type=source_type,
            )
            result.append(query)
        return result

    def _get_first_and_last_indices(
        self, agg_spec: TileBasedAggregationSpec
    ) -> Tuple[Optional[Expression], Expression]:
        """
        Get the first and last tile indices required to compute the feature

        Parameters
        ----------
        agg_spec: TileBasedAggregationSpec
            Aggregation specification

        Returns
        -------
        Tuple[Optional[Expression], Expression]
        """
        point_in_time_expr = self._get_point_in_time_expr()
        window_size = agg_spec.window
        first_index, last_index = calculate_first_and_last_tile_indices(
            adapter=self.adapter,
            point_in_time_expr=point_in_time_expr,
            window_size=window_size,
            frequency=agg_spec.frequency,
            time_modulo_frequency=agg_spec.time_modulo_frequency,
        )
        return first_index, last_index

    def _construct_online_store_precompute_query(
        self,
        params: PrecomputeQueryParams,
        source_type: SourceType,
    ) -> OnlineStoreComputeQueryModel:
        planner = FeatureExecutionPlanner(
            params.agg_spec.pruned_graph,
            source_type=source_type,
            is_online_serving=False,
        )
        plan = planner.generate_plan([params.agg_spec.pruned_node])

        result_type = self.adapter.get_physical_type_from_dtype(params.agg_spec.dtype)
        table_name = get_online_store_table_name(
            set(params.agg_spec.entity_ids if params.agg_spec.entity_ids is not None else []),
            result_type=result_type,
        )
        sql_expr = self._convert_to_online_store_schema(
            plan.construct_combined_sql(
                request_table_name=REQUEST_TABLE_NAME,
                point_in_time_column=SpecialColumnName.POINT_IN_TIME,
                request_table_columns=params.universe.columns,
                prior_cte_statements=[(REQUEST_TABLE_NAME, params.universe.expr)],
                exclude_post_aggregation=True,
            ),
            serving_names=sorted(params.agg_spec.serving_names),
            result_name=params.agg_spec.agg_result_name,
        )
        sql = sql_to_string(sql_expr, source_type)

        return OnlineStoreComputeQueryModel(
            sql=sql,
            tile_id=params.agg_spec.tile_table_id,
            aggregation_id=params.agg_spec.aggregation_id,
            table_name=table_name,
            result_name=params.agg_spec.agg_result_name,
            result_type=result_type,
            serving_names=sorted(params.agg_spec.serving_names),
        )

    @staticmethod
    def _convert_to_online_store_schema(
        aggregation_expr: expressions.Select,
        serving_names: list[str],
        result_name: str,
    ) -> expressions.Select:
        """
        Convert the aggregation expression to the online store schema in long format with fixed
        columns: SERVING_NAME_1[, ..., SERVING_NAME_N], AGGREGATION_RESULT_NAME, VALUE

        Parameters
        ----------
        aggregation_expr: expressions.Select
            Aggregation expression
        serving_names: list[str]
            Serving names
        result_name: str
            Aggregation result name

        Returns
        -------
        expressions.Select
        """
        output_expr = select().from_(aggregation_expr.subquery())

        output_expr = output_expr.select(
            *[quoted_identifier(serving_name) for serving_name in serving_names]
        )

        output_expr = output_expr.select(
            alias_(
                make_literal_value(result_name),
                alias=InternalName.ONLINE_STORE_RESULT_NAME_COLUMN,
                quoted=True,
            ),
            alias_(
                quoted_identifier(result_name),
                alias=InternalName.ONLINE_STORE_VALUE_COLUMN,
                quoted=True,
            ),
        )

        return output_expr

    def _construct_online_store_universe(
        self,
        agg_spec: TileBasedAggregationSpec,
    ) -> OnlineStoreUniverse:
        """
        Construct SQL expression that extracts the universe for online store. The result of this SQL
        contains a point in time column, so it can be used directly as the request table.

        Parameters
        ----------
        agg_spec: TileBasedAggregationSpec
            Aggregation specification

        Returns
        -------
        OnlineStoreUniverse
        """
        first_index, last_index = self._get_first_and_last_indices(agg_spec)

        tile_id = agg_spec.tile_table_id
        serving_names = agg_spec.serving_names
        keys = agg_spec.keys

        filter_conditions: list[Expression] = []
        if first_index is not None:
            filter_conditions.append(expressions.GTE(this="INDEX", expression=first_index))
        filter_conditions.append(expressions.LT(this="INDEX", expression=last_index))

        expr = (
            select(
                expressions.alias_(self._get_point_in_time_expr(), SpecialColumnName.POINT_IN_TIME),
                *[
                    expressions.alias_(
                        quoted_identifier(key_col), quoted_identifier(serving_name_col)
                    )
                    for key_col, serving_name_col in zip(keys, serving_names)
                ],
            )
            .distinct()
            .from_(tile_id)
            .where(expressions.and_(*filter_conditions))
        )
        universe_columns = [SpecialColumnName.POINT_IN_TIME.value] + serving_names

        return OnlineStoreUniverse(expr=expr, columns=universe_columns)

    def _update(self, graph: QueryGraph, node: Node) -> None:
        """
        Update state given a query graph node

        Parameters
        ----------
        graph: QueryGraph
            Query graph
        node: Node
            Query graph node
        """
        groupby_nodes = list(graph.iterate_nodes(node, NodeType.GROUPBY))
        for groupby_node in groupby_nodes:
            agg_specs = TileBasedAggregationSpec.from_groupby_query_node(
                graph, groupby_node, self.adapter
            )
            for agg_spec in agg_specs:
                universe = self._construct_online_store_universe(agg_spec)
                self.params_by_agg_result_name[agg_spec.agg_result_name] = PrecomputeQueryParams(
                    agg_spec=agg_spec,
                    universe=universe,
                )

    @classmethod
    def _get_point_in_time_expr(cls) -> Expression:
        return cast(
            Expression,
            parse_one(f"CAST({InternalName.POINT_IN_TIME_SQL_PLACEHOLDER} AS TIMESTAMP)"),
        )


def get_online_store_precompute_queries(
    graph: QueryGraph,
    node: Node,
    source_type: SourceType,
) -> list[OnlineStoreComputeQueryModel]:
    """
    Construct the SQL code that can be scheduled for online store feature pre-computation

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    node : Node
        Query graph node
    source_type : SourceType
        Source type information

    Returns
    -------
    list[OnlineStoreComputeQueryModel]
    """
    universe_plan = OnlineStorePrecomputePlan(graph, node, adapter=get_sql_adapter(source_type))
    queries = universe_plan.construct_online_store_precompute_queries(source_type=source_type)
    return queries
