"""
SQL generation for as-at aggregation
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, TypeVar, cast

from sqlglot import expressions
from sqlglot.expressions import Select, select

from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    CommonTable,
    LeftJoinableSubquery,
    NonTileBasedAggregator,
)
from featurebyte.query_graph.sql.aggregator.request_table import RequestTablePlan
from featurebyte.query_graph.sql.asat_helper import (
    ensure_end_timestamp_column,
    get_record_validity_condition,
)
from featurebyte.query_graph.sql.common import (
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.groupby_helper import GroupbyColumn, GroupbyKey, get_groupby_expr
from featurebyte.query_graph.sql.offset import OffsetDirection, add_offset_to_timestamp
from featurebyte.query_graph.sql.specifications.base_aggregate_asat import BaseAggregateAsAtSpec

AsAtSpecT = TypeVar("AsAtSpecT", bound=BaseAggregateAsAtSpec)


class BaseAsAtAggregator(NonTileBasedAggregator[AsAtSpecT]):
    """
    AsAtAggregation is responsible for generating SQL for as at aggregation
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.request_table_plan = RequestTablePlan(is_time_aware=True)

    @classmethod
    @abstractmethod
    def get_offset_direction(cls) -> OffsetDirection:
        """
        Get the direction when applying the offset to point-in-time
        """

    def additional_update(self, aggregation_spec: AsAtSpecT) -> None:
        """
        Update internal states to account for aggregation spec

        Parameters
        ----------
        aggregation_spec: AsAtSpecT
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

    def _get_aggregation_subquery(self, specs: list[AsAtSpecT]) -> LeftJoinableSubquery:
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
        end_timestamp_column, scd_expr = ensure_end_timestamp_column(
            adapter=self.adapter,
            effective_timestamp_column=spec.parameters.effective_timestamp_column,
            effective_timestamp_schema=spec.parameters.effective_timestamp_schema,
            natural_key_column=cast(str, spec.parameters.natural_key_column),
            end_timestamp_column=spec.parameters.end_timestamp_column,
            source_expr=spec.source_expr,
        )

        if spec.parameters.keys:
            join_key_condition = expressions.and_(*[
                expressions.EQ(
                    this=get_qualified_column_identifier(serving_name, "REQ"),
                    expression=get_qualified_column_identifier(key, "SCD"),
                )
                for serving_name, key in zip(spec.serving_names, spec.parameters.keys)
            ])
        else:
            join_key_condition = expressions.true()

        # Use offset adjusted point in time to join with SCD table if any
        if spec.parameters.offset is None:
            point_in_time_expr = get_qualified_column_identifier(
                SpecialColumnName.POINT_IN_TIME, "REQ"
            )
        else:
            point_in_time_expr = add_offset_to_timestamp(
                adapter=self.adapter,
                timestamp_expr=get_qualified_column_identifier(
                    SpecialColumnName.POINT_IN_TIME, "REQ"
                ),
                offset=spec.parameters.offset,
                offset_direction=self.get_offset_direction(),
            )
        point_in_time_expr = self.adapter.normalize_timestamp_before_comparison(point_in_time_expr)

        # Only join records from the SCD table that are valid as at point in time
        record_validity_condition = get_record_validity_condition(
            adapter=self.adapter,
            effective_timestamp_column=spec.parameters.effective_timestamp_column,
            effective_timestamp_schema=spec.parameters.effective_timestamp_schema,
            end_timestamp_column=end_timestamp_column,
            end_timestamp_schema=spec.parameters.end_timestamp_schema,
            point_in_time_expr=point_in_time_expr,
        )
        join_condition = expressions.and_(join_key_condition, record_validity_condition)

        groupby_keys = [
            GroupbyKey(
                expr=get_qualified_column_identifier(SpecialColumnName.POINT_IN_TIME, "REQ"),
                name=SpecialColumnName.POINT_IN_TIME,
            )
        ] + [
            GroupbyKey(
                expr=get_qualified_column_identifier(serving_name, "REQ"),
                name=serving_name,
            )
            for serving_name in spec.serving_names
        ]
        value_by = (
            GroupbyKey(
                expr=get_qualified_column_identifier(spec.parameters.value_by, "SCD"),
                name=spec.parameters.value_by,
            )
            if spec.parameters.value_by
            else None
        )
        groupby_columns = [
            GroupbyColumn(
                agg_func=s.parameters.agg_func,
                parent_expr=(
                    get_qualified_column_identifier(s.parameters.parent, "SCD")
                    if s.parameters.parent
                    else None
                ),
                result_name=s.agg_result_name,
                parent_dtype=s.parent_dtype,
                parent_cols=(
                    [get_qualified_column_identifier(s.parameters.parent, "SCD")]
                    if s.parameters.parent
                    else []
                ),
            )
            for s in specs
        ]
        groupby_input_expr = (
            select()
            .from_(
                expressions.Table(
                    this=quoted_identifier(self.request_table_plan.get_request_table_name(spec)),
                    alias="REQ",
                )
            )
            .join(
                scd_expr.subquery(alias="SCD"),
                join_type="inner",
                on=join_condition,
            )
        )
        aggregate_asat_expr = get_groupby_expr(
            input_expr=groupby_input_expr,
            groupby_keys=groupby_keys,
            groupby_columns=groupby_columns,
            value_by=value_by,
            adapter=self.adapter,
        )
        return LeftJoinableSubquery(
            expr=aggregate_asat_expr,
            column_names=[s.agg_result_name for s in specs],
            join_keys=[SpecialColumnName.POINT_IN_TIME.value] + spec.serving_names,
        )

    def get_common_table_expressions(self, request_table_name: str) -> list[CommonTable]:
        return self.request_table_plan.construct_request_table_ctes(request_table_name)
