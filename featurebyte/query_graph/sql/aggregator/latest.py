"""
SQL generation for latest feature without a window
"""

from __future__ import annotations

import sys
from typing import Any

from sqlglot.expressions import Select

from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    CommonTable,
    TileBasedAggregator,
)
from featurebyte.query_graph.sql.aggregator.window import TileBasedAggregationSpecSet
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.scd_helper import Table, get_scd_join_expr
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec
from featurebyte.query_graph.sql.tile_util import calculate_last_tile_index_expr

sys.setrecursionlimit(10000)


class LatestAggregator(TileBasedAggregator):
    """
    LatestAggregator is responsible for SQL generation for latest value aggregation without a window
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.specs_set = TileBasedAggregationSpecSet()

    def additional_update(self, aggregation_spec: TileBasedAggregationSpec) -> None:
        """
        Update internal states given a TileBasedAggregationSpec

        Parameters
        ----------
        aggregation_spec: TileBasedAggregationSpec
            Aggregation specification
        """
        assert aggregation_spec.window is None
        self.specs_set.add_aggregation_spec(aggregation_spec)

    def update_aggregation_table_expr(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        # Note: always retrieve from tile table, even for internal store online retrieval
        return self.update_aggregation_table_expr_offline(
            table_expr=table_expr,
            point_in_time_column=point_in_time_column,
            current_columns=current_columns,
            current_query_index=current_query_index,
        )

    def update_aggregation_table_expr_offline(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        all_agg_result_names = []

        for specs in self.specs_set.get_grouped_aggregation_specs():
            last_tile_index_expr = calculate_last_tile_index_expr(
                adapter=self.adapter,
                point_in_time_expr=quoted_identifier(point_in_time_column),
                frequency=specs[0].frequency,
                time_modulo_frequency=specs[0].time_modulo_frequency,
                offset=specs[0].offset,
            )
            left_table = Table(
                expr=table_expr,
                timestamp_column=last_tile_index_expr,
                timestamp_schema=None,
                join_keys=specs[0].serving_names,
                input_columns=current_columns,
                output_columns=current_columns,
            )

            agg_result_names = [spec.agg_result_name for spec in specs]
            tile_value_columns = []
            for spec in specs:
                # latest method's tile should only have one tile value
                assert len(spec.tile_value_columns) == 1
                tile_value_columns.extend(spec.tile_value_columns)

            right_table = Table(
                expr=specs[0].tile_table_id,
                timestamp_column="INDEX",
                timestamp_schema=None,
                join_keys=specs[0].keys,
                input_columns=tile_value_columns,
                output_columns=agg_result_names,
            )

            table_expr = get_scd_join_expr(
                left_table,
                right_table,
                join_type="left",
                adapter=self.adapter,
                allow_exact_match=False,
                quote_right_input_columns=False,
                convert_timestamps_to_utc=False,
            )
            current_columns = current_columns + agg_result_names
            all_agg_result_names.extend(agg_result_names)

        if all_agg_result_names:
            table_expr = self._wrap_in_nested_query(table_expr=table_expr, columns=current_columns)

        return AggregationResult(
            updated_table_expr=table_expr,
            updated_index=current_query_index,
            column_names=all_agg_result_names,
        )

    def get_common_table_expressions(
        self,
        request_table_name: str,
    ) -> list[CommonTable]:
        return []
