"""
SQL generation for as-at aggregation
"""
from __future__ import annotations

from typing import Any, cast

from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    LeftJoinableSubquery,
    NonTileBasedAggregator,
)
from featurebyte.query_graph.sql.aggregator.request_table import RequestTablePlan
from featurebyte.query_graph.sql.common import (
    CteStatements,
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.groupby_helper import get_aggregation_expression
from featurebyte.query_graph.sql.scd_helper import END_TS, augment_scd_table_with_end_timestamp
from featurebyte.query_graph.sql.specs import AggregateAsAtSpec


class AsAtAggregator(NonTileBasedAggregator[AggregateAsAtSpec]):
    """
    AsAtAggregation is responsible for generating SQL for as at aggregation
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.request_table_plan = RequestTablePlan(is_time_aware=True)

    def additional_update(self, aggregation_spec: AggregateAsAtSpec) -> None:
        """
        Update internal states to account for aggregation spec

        Parameters
        ----------
        aggregation_spec: AggregateAsAtSpec
            Aggregation spec
        """
        self.request_table_plan.add_aggregation_spec(aggregation_spec)

    def update_aggregation_table_expr(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        queries = []
        for specs in self.grouped_specs.values():
            query = self._get_aggregation_subquery(specs)
            queries.append(query)

        return self._update_with_left_joins(
            table_expr=table_expr, current_query_index=current_query_index, queries=queries
        )

    def _get_aggregation_subquery(self, specs: list[AggregateAsAtSpec]) -> LeftJoinableSubquery:
        """
        Get aggregation subquery that performs the asat aggregation. The list of aggregation
        specifications provided can be done in a single groupby operation.

        Parameters
        ----------
        specs: list[AggregationAsAtSpec]
            Aggregation specifications

        Returns
        -------
        LeftJoinableSubquery
        """
        spec = specs[0]
        if spec.parameters.end_timestamp_column is None:
            scd_expr = augment_scd_table_with_end_timestamp(
                table_expr=spec.source_expr,
                effective_timestamp_column=spec.parameters.effective_timestamp_column,
                natural_key_column=cast(str, spec.parameters.natural_key_column),
            )
            end_timestamp_column = END_TS
        else:
            scd_expr = spec.source_expr
            end_timestamp_column = spec.parameters.end_timestamp_column

        if spec.parameters.keys:
            join_key_condition = expressions.and_(
                *[
                    expressions.EQ(
                        this=get_qualified_column_identifier(serving_name, "REQ"),
                        expression=get_qualified_column_identifier(key, "SCD"),
                    )
                    for serving_name, key in zip(spec.serving_names, spec.parameters.keys)
                ]
            )
        else:
            join_key_condition = expressions.true()

        # Only join records from the SCD table that are valid as at point in time
        record_validity_condition = expressions.and_(
            # SCD.effective_timestamp_column <= REQ.POINT_IN_TIME; i.e. record became effective
            # at or before point in time
            expressions.LTE(
                this=get_qualified_column_identifier(
                    spec.parameters.effective_timestamp_column, "SCD"
                ),
                expression=get_qualified_column_identifier(SpecialColumnName.POINT_IN_TIME, "REQ"),
            ),
            expressions.or_(
                # SCD.end_timestamp_column > REQ.POINT_IN_TIME; i.e. record has not yet been
                # invalidated as at the point in time, but will be at a future time
                expressions.GT(
                    this=get_qualified_column_identifier(end_timestamp_column, "SCD"),
                    expression=get_qualified_column_identifier(
                        SpecialColumnName.POINT_IN_TIME, "REQ"
                    ),
                ),
                # SCD.end_timestamp_column IS NULL; i.e. record is current
                expressions.Is(
                    this=get_qualified_column_identifier(end_timestamp_column, "SCD"),
                    expression=expressions.Null(),
                ),
            ),
        )
        join_condition = expressions.and_(join_key_condition, record_validity_condition)

        groupby_keys = [get_qualified_column_identifier(SpecialColumnName.POINT_IN_TIME, "REQ")] + [
            get_qualified_column_identifier(serving_name, "REQ")
            for serving_name in spec.serving_names
        ]
        agg_exprs = [
            alias_(
                get_aggregation_expression(
                    agg_func=s.parameters.agg_func,
                    input_column=(
                        get_qualified_column_identifier(s.parameters.parent, "SCD")
                        if s.parameters.parent
                        else None
                    ),
                ),
                alias=s.agg_result_name,
                quoted=True,
            )
            for s in specs
        ]
        request_table_name = self.request_table_plan.get_request_table_name(spec)
        aggregate_asat_expr = (
            select(*groupby_keys, *agg_exprs)
            .from_(
                expressions.Table(
                    this=quoted_identifier(request_table_name),
                    alias="REQ",
                )
            )
            .join(
                scd_expr.subquery(alias="SCD"),
                join_type="inner",
                on=join_condition,
            )
            .group_by(*groupby_keys)
        )

        return LeftJoinableSubquery(
            expr=aggregate_asat_expr,
            column_names=[s.agg_result_name for s in specs],
            join_keys=[SpecialColumnName.POINT_IN_TIME.value] + spec.serving_names,
        )

    def get_common_table_expressions(self, request_table_name: str) -> CteStatements:
        return self.request_table_plan.construct_request_table_ctes(request_table_name)
