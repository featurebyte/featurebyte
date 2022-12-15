"""
SQL generation for lookup features
"""
from __future__ import annotations

from typing import Iterable, Tuple

from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    Aggregator,
    LeftJoinableSubquery,
)
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier
from featurebyte.query_graph.sql.scd_helper import Table, get_scd_join_expr
from featurebyte.query_graph.sql.specs import LookupSpec


class LookupAggregator(Aggregator):
    """
    LookupAggregator is responsible for generating SQL for lookup features
    """

    def __init__(self, *args, **kwargs) -> None:
        # The keys in these dicts are unique identifiers (based on LookupSpec's source_hash) that
        # determine which lookup features can be retrieved in a single join
        super().__init__(*args, **kwargs)
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

    def iterate_grouped_lookup_specs(self, is_scd: bool) -> Iterable[list[LookupSpec]]:
        """
        Iterate over groups of LookupSpec filtering by time awareness. All the LookupSpecs in a
        group can be looked up using the same join.

        Parameters
        ----------
        is_scd: bool
            If true, only yields time aware LookupSpecs, and vice versa

        Yields
        ------
        list[LookupSpec]
            Group of LookupSpec as a list
        """
        for specs in self.grouped_lookup_specs.values():
            is_specs_time_aware = specs[0].scd_parameters is not None
            if is_scd and is_specs_time_aware:
                yield specs
            if not is_scd and not is_specs_time_aware:
                yield specs

    def get_non_time_based_lookups(self) -> list[LeftJoinableSubquery]:
        """
        Get non-time based lookups queries

        Returns
        -------
        list[LeftJoinableSubquery]
        """

        out = []

        for specs in self.iterate_grouped_lookup_specs(is_scd=False):

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
        current_query_index: int,
    ) -> AggregationResult:

        # SCD lookup
        table_expr, scd_agg_result_names = self._update_with_scd_lookups(
            table_expr=table_expr,
            point_in_time_column=point_in_time_column,
            current_columns=current_columns,
        )

        # Non-time based lookup
        queries = self.get_non_time_based_lookups()
        result = self._update_with_left_joins(
            table_expr=table_expr, current_query_index=current_query_index, queries=queries
        )

        # Update result column names to account for both types of lookups
        result.column_names = scd_agg_result_names + result.column_names
        return result

    def _update_with_scd_lookups(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
    ) -> Tuple[Select, list[str]]:
        """
        Generates sql for SCD lookup and returns the updated table and added columns

        Parameters
        ----------
        table_expr: Select
            The table expression to update
        point_in_time_column: str
            Point in time column name
        current_columns: list[str]
            List of column names in the table

        Returns
        -------
        Tuple[Select, list[str]]
            First element is the updated table_expr. Second element is the new columns added
        """

        scd_agg_result_names = []

        for lookup_specs in self.iterate_grouped_lookup_specs(is_scd=True):

            left_table = Table(
                expr=table_expr,
                timestamp_column=point_in_time_column,
                join_key=lookup_specs[0].serving_names[0],
                input_columns=current_columns,
                output_columns=current_columns,
            )

            agg_result_names = [spec.agg_result_name for spec in lookup_specs]
            scd_parameters = lookup_specs[0].scd_parameters
            assert scd_parameters is not None
            right_table = Table(
                expr=lookup_specs[0].source_expr,
                timestamp_column=scd_parameters.effective_timestamp_column,
                join_key=lookup_specs[0].entity_column,
                input_columns=[spec.input_column_name for spec in lookup_specs],
                output_columns=agg_result_names,
            )
            table_expr = get_scd_join_expr(
                left_table,
                right_table,
                join_type="left",
                adapter=self.adapter,
                offset=scd_parameters.offset,
            )

            current_columns = current_columns + agg_result_names
            scd_agg_result_names.extend(agg_result_names)

        if scd_agg_result_names:
            # If any SCD lookup is performed, set up the REQ alias again so that subsequent joins
            # using other aggregators can work
            table_expr = select(
                *[
                    alias_(get_qualified_column_identifier(col, "REQ"), col, quoted=True)
                    for col in current_columns
                ]
            ).from_(table_expr.subquery(alias="REQ"))

        return table_expr, scd_agg_result_names

    def get_common_table_expressions(
        self, request_table_name: str
    ) -> list[tuple[str, expressions.Select]]:
        _ = request_table_name
        return []
