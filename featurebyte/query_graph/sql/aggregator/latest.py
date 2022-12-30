from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.query_graph.sql.aggregator.base import AggregationResult, Aggregator
from featurebyte.query_graph.sql.aggregator.window import TileBasedAggregationSpecSet
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier
from featurebyte.query_graph.sql.scd_helper import Table, get_scd_join_expr
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec
from featurebyte.query_graph.sql.tile_util import calculate_last_tile_index_expr


class LatestAggregator(Aggregator):
    """
    LatestAggregator is responsible for SQL generation for latest value aggregation without a window
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.specs_set = TileBasedAggregationSpecSet()

    def update(self, spec: TileBasedAggregationSpec) -> None:
        """
        Update internal states given a TileBasedAggregationSpec
        """
        assert spec.window is None
        self.specs_set.add_aggregation_spec(spec)

    def get_required_serving_names(self) -> set[str]:
        out = set()
        for specs in self.specs_set.get_grouped_aggregation_specs():
            out.update(specs[0].serving_names)
        return out

    def update_aggregation_table_expr(
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
                exclusive=False,
            )
            left_table = Table(
                expr=table_expr,
                timestamp_column=last_tile_index_expr,
                join_key=specs[0].serving_names[0],
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
                join_key=specs[0].keys[0],
                input_columns=tile_value_columns,
                output_columns=agg_result_names,
            )

            table_expr = get_scd_join_expr(
                left_table,
                right_table,
                join_type="left",
                adapter=self.adapter,
                quote_right_input_columns=False,
            )
            current_columns = current_columns + agg_result_names
            all_agg_result_names.extend(agg_result_names)

        if all_agg_result_names:
            # If any SCD lookup is performed, set up the REQ alias again so that subsequent joins
            # using other aggregators can work
            table_expr = select(
                *[
                    alias_(get_qualified_column_identifier(col, "REQ"), col, quoted=True)
                    for col in current_columns
                ]
            ).from_(table_expr.subquery(alias="REQ"))

        return AggregationResult(
            updated_table_expr=table_expr,
            updated_index=current_query_index,
            column_names=all_agg_result_names,
        )

    def get_common_table_expressions(
        self,
        request_table_name: str,
    ) -> list[tuple[str, expressions.Select]]:
        return []
