"""
SQL generation for online serving
"""
from __future__ import annotations

from typing import Any, List, Tuple

from collections import defaultdict

from sqlglot import Expression, expressions

from featurebyte.enum import SourceType, SpecialColumnName
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.adapter import BaseAdapter, get_sql_adapter
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME, quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.sql.specs import PointInTimeAggregationSpec
from featurebyte.query_graph.sql.tile_util import calculate_first_and_last_tile_indices


class OnlineStoreUniversePlan:
    """
    OnlineStoreUniversePlan is responsible for extracting universe for online store feature
    pre-computation

    Parameters
    ----------
    graph: QueryGraph
        Query graph
    adapter: BaseAdapter
        Instance of BaseAdapter for engine specific sql generation
    """

    def __init__(self, graph: QueryGraph, adapter: BaseAdapter):
        self.graph = graph
        self.adapter = adapter
        self.max_window_size_by_tile_id: dict[str, int] = defaultdict(int)
        self.params_by_tile_id: dict[str, Any] = {}

    def update(self, node: Node) -> None:
        """
        Update state given a query graph node

        Parameters
        ----------
        node : Node
            Query graph node
        """
        groupby_nodes = list(self.graph.iterate_nodes(node, NodeType.GROUPBY))
        for groupby_node in groupby_nodes:
            agg_specs = PointInTimeAggregationSpec.from_groupby_query_node(groupby_node)
            for agg_spec in agg_specs:
                tile_id = agg_spec.tile_table_id
                self.max_window_size_by_tile_id[tile_id] = max(
                    agg_spec.window, self.max_window_size_by_tile_id[tile_id]
                )
                self.params_by_tile_id[tile_id] = {
                    "keys": agg_spec.keys,
                    "serving_names": agg_spec.serving_names,
                    "frequency": agg_spec.frequency,
                    "time_modulo_frequency": agg_spec.time_modulo_frequency,
                }

    def get_first_and_last_indices_by_tile_id(self) -> List[Tuple[str, Expression, Expression]]:
        """
        Get the first and last tile indices required to compute the feature

        Returns
        -------
        List[Tuple[str, Expression, Expression]]
        """
        out = []
        point_in_time_expr = self._get_point_in_time_expr()
        for tile_id, window_size in self.max_window_size_by_tile_id.items():
            params_dict = self.params_by_tile_id[tile_id]
            first_index, last_index = calculate_first_and_last_tile_indices(
                adapter=self.adapter,
                point_in_time_expr=point_in_time_expr,
                window_size=window_size,
                frequency=params_dict["frequency"],
                time_modulo_frequency=params_dict["time_modulo_frequency"],
            )
            out.append((tile_id, first_index, last_index))
        return out

    def construct_online_store_universe(self) -> Tuple[expressions.Select, List[str]]:
        """
        Construct SQL expression that extracts the universe for online store. The result of this SQL
        contains a point in time column, so it can be used directly as the request table.

        Returns
        -------
        Tuple[expressions.Select, List[str]]
        """

        first_and_last_indices_by_tile_id = self.get_first_and_last_indices_by_tile_id()

        # TODO: Handle features derived from multiple features
        #
        # If there are more than one tile tables, the feature is a complex feature derived from
        # multiple features with entities that are identical or have a parent child relationship,
        # from the same EventData / ItemData. For the parent child relationship case, we need to
        # identify the least ancestral entity and use that to define the universe. For now, assert
        # that the feature is simple.
        if len(first_and_last_indices_by_tile_id) != 1:
            raise NotImplementedError()

        tile_id, first_index, last_index = first_and_last_indices_by_tile_id[0]
        params = self.params_by_tile_id[tile_id]
        serving_names = params["serving_names"]
        keys = params["keys"]

        filter_condition = expressions.and_(
            expressions.GTE(this="INDEX", expression=first_index),
            expressions.LT(this="INDEX", expression=last_index),
        )
        expr = (
            expressions.Select(distinct=True)
            .select(
                expressions.alias_(self._get_point_in_time_expr(), SpecialColumnName.POINT_IN_TIME),
                *[
                    expressions.alias_(
                        quoted_identifier(key_col), quoted_identifier(serving_name_col)
                    )
                    for key_col, serving_name_col in zip(keys, serving_names)
                ],
            )
            .from_(tile_id)
            .where(filter_condition)
        )
        universe_columns = [SpecialColumnName.POINT_IN_TIME] + serving_names

        return expr, universe_columns

    @classmethod
    def _get_point_in_time_expr(cls) -> Expression:
        return expressions.Anonymous(this="SYSDATE")


def get_online_store_feature_compute_sql(
    graph: QueryGraph,
    node: Node,
    source_type: SourceType,
) -> str:
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
    str
    """
    planner = FeatureExecutionPlanner(graph, source_type=source_type)
    plan = planner.generate_plan([node])

    universe_plan = OnlineStoreUniversePlan(graph, adapter=get_sql_adapter(source_type))
    universe_plan.update(node)
    universe_expr, universe_columns = universe_plan.construct_online_store_universe()

    sql_expr = plan.construct_combined_sql(
        request_table_name=REQUEST_TABLE_NAME,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=universe_columns,
        prior_cte_statements=[(REQUEST_TABLE_NAME, universe_expr)],
    )

    return sql_to_string(sql_expr, source_type)
