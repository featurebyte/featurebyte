"""
SQL generation for online serving
"""
from __future__ import annotations

from typing import List, Tuple

from collections import defaultdict

from sqlglot import Expression, expressions

from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import quoted_identifier
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
        self.max_window_size_by_tile_id = defaultdict(int)
        self.params_by_tile_id = {}

    def update(self, node: Node) -> None:
        groupby_nodes = list(self.graph.iterate_nodes(node, NodeType.GROUPBY))
        for node in groupby_nodes:
            agg_specs = PointInTimeAggregationSpec.from_groupby_query_node(node)
            for agg_spec in agg_specs:
                tile_id = agg_spec.tile_table_id
                self.max_window_size_by_tile_id[tile_id] = max(
                    agg_spec.window, self.max_window_size_by_tile_id[tile_id]
                )
                self.params_by_tile_id[tile_id] = {
                    "entity_columns": agg_spec.keys,
                    "frequency": agg_spec.frequency,
                    "time_modulo_frequency": agg_spec.time_modulo_frequency,
                }

    def get_first_and_last_indices_by_tile_id(self) -> List[Tuple[str, Expression, Expression]]:
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

    def construct_online_store_universe_expr(self) -> Expression:

        first_and_last_indices_by_tile_id = self.get_first_and_last_indices_by_tile_id()

        # TODO: Handle features derived from multiple features
        #
        # If there are more than one tile tables, the feature is a complex feature derived from
        # multiple features with entities that are identical or have a parent child relationship,
        # from the same EventData / ItemData. For the parent child relationship case, we need to
        # identify the least ancestral entity and use that to define the universe. For now, assert
        # that the feature is simple.
        assert len(first_and_last_indices_by_tile_id) == 1

        tile_id, first_index, last_index = first_and_last_indices_by_tile_id[0]
        entity_columns = self.params_by_tile_id[tile_id]["entity_columns"]

        filter_condition = expressions.and_(
            expressions.GTE(this="INDEX", expression=first_index),
            expressions.LT(this="INDEX", expression=last_index),
        )
        expr = (
            expressions.Select(distinct=True)
            .select(
                expressions.alias_(self._get_point_in_time_expr(), SpecialColumnName.POINT_IN_TIME),
                *[quoted_identifier(col) for col in entity_columns],
            )
            .from_(tile_id)
            .where(filter_condition)
        )

        return expr

    @classmethod
    def _get_point_in_time_expr(cls) -> Expression:
        return expressions.Anonymous(this="SYSDATE")
