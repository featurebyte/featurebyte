"""
SQL generation for lookup features
"""
from __future__ import annotations

from typing import Iterable

from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    Aggregator,
    LeftJoinableSubquery,
)
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.specs import LookupSpec


class LookupAggregator(Aggregator):
    """
    LookupAggregator is responsible for generating SQL for lookup features
    """

    def __init__(self) -> None:
        # The keys in these dicts are unique identifiers (based on LookupSpec's source_hash) that
        # determine which lookup features can be retrieved in a single join
        self.grouped_lookup_specs: dict[str, list[LookupSpec]] = {}
        self.grouped_agg_result_names: dict[str, set[str]] = {}

    def update(self, spec: LookupSpec) -> None:
        """
        Update state to account for LookupSpec

        Main work is to group lookup features from the same source together (identified by a
        combination of source sql and entity column) so that they can be looked up with a single
        join.

        Parameters
        ----------
        spec: LookupSpec
            Lookup specification
        """
        key = spec.source_hash

        if key not in self.grouped_lookup_specs:
            self.grouped_agg_result_names[key] = set()
            self.grouped_lookup_specs[key] = []

        if spec.agg_result_name in self.grouped_agg_result_names[key]:
            # Skip updating if the spec produces a result that was seen before. One example this can
            # occur is when the same lookup is used more than once in a feature list by multiple
            # features. In that case, they can all share the same lookup result.
            return

        self.grouped_agg_result_names[key].add(spec.agg_result_name)
        self.grouped_lookup_specs[key].append(spec)

    @property
    def lookup_specs(self) -> Iterable[LookupSpec]:
        """
        Yields a list of LookupSpec recorded

        Yields
        ------
        LookupSpec
            Instance of LookupSpec
        """
        for specs in self.grouped_lookup_specs.values():
            yield from specs

    def get_required_serving_names(self) -> set[str]:
        out = set()
        for spec in self.lookup_specs:
            out.update(spec.serving_names)
        return out

    def get_non_time_based_lookups(self) -> list[LeftJoinableSubquery]:
        """
        Get non-time based lookups queries

        Returns
        -------
        list[LeftJoinableSubquery]
        """

        out = []

        for specs in self.grouped_lookup_specs.values():

            entity_column = specs[0].entity_column
            serving_name = specs[0].serving_names[0]
            source_expr = specs[0].source_expr

            agg_expr = select(
                alias_(quoted_identifier(entity_column), alias=serving_name, quoted=True),
                *[
                    alias_(
                        quoted_identifier(spec.input_column_name),
                        alias=spec.agg_result_name,
                        quoted=True,
                    )
                    for spec in specs
                ],
            ).from_(source_expr.subquery())

            result = LeftJoinableSubquery(
                expr=agg_expr,
                column_names=[spec.agg_result_name for spec in specs],
                join_keys=[serving_name],
            )
            out.append(result)

        return out

    def update_aggregation_table_expr(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_index: int,
    ) -> AggregationResult:

        queries = self.get_non_time_based_lookups()

        return self._update_with_left_joins(
            table_expr=table_expr, current_index=current_index, queries=queries
        )

    def get_common_table_expressions(
        self, request_table_name: str
    ) -> list[tuple[str, expressions.Select]]:
        _ = request_table_name
        return []
