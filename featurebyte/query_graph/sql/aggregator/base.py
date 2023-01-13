"""
Base class for aggregation SQL generators
"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from abc import ABC, abstractmethod
from dataclasses import dataclass

from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier
from featurebyte.query_graph.sql.specs import AggregationSpec, NonTileBasedAggregationSpec

AggregationSpecT = TypeVar("AggregationSpecT", bound=AggregationSpec)
NonTileBasedAggregationSpecT = TypeVar(
    "NonTileBasedAggregationSpecT", bound=NonTileBasedAggregationSpec
)


@dataclass
class AggregationResult:
    """
    Representation of aggregation result from an instance of Aggregator
    """

    updated_table_expr: Select
    updated_index: int
    column_names: list[str]


@dataclass
class LeftJoinableSubquery:
    """
    Representation of a subquery that can be left joined with the request table
    """

    expr: expressions.Select
    column_names: list[str]
    join_keys: list[str]


class Aggregator(Generic[AggregationSpecT], ABC):
    """
    Base class of all aggregators
    """

    def __init__(self, source_type: SourceType, is_online_serving: bool = False):
        self.source_type = source_type
        self.adapter = get_sql_adapter(source_type)
        self.is_online_serving = is_online_serving

    @abstractmethod
    def get_required_serving_names(self) -> set[str]:
        """
        Get the set of required serving names

        Returns
        -------
        set[str]
        """

    @abstractmethod
    def update(self, aggregation_spec: AggregationSpecT) -> None:
        """
        Update internal states given an AggregationSpec

        Parameters
        ----------
        aggregation_spec: AggregationSpecT
            Aggregation specification
        """

    @abstractmethod
    def update_aggregation_table_expr(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        """
        Update a provided table with aggregated results left joined into it

        Parameters
        ----------
        table_expr: Select
            The table expression to update. This is typically the temporary aggregation table that
            has the same number of rows as the request table, referred to in the final feature sql
            as _FB_AGGREGATED
        point_in_time_column: str
            Point in time column name
        current_columns: list[str]
            List of column names in the current table
        current_query_index: int
            A running integer to be used to construct new table aliases

        Returns
        -------
        AggregationResult
        """

    def _update_with_left_joins(
        self,
        table_expr: Select,
        current_query_index: int,
        queries: list[LeftJoinableSubquery],
    ) -> AggregationResult:
        """
        Update table_expr by left joining with a list of LeftJoinableSubquery

        Parameters
        ----------
        table_expr: Select
            Table expression to update
        current_query_index: int
            A running integer to be used to construct new table aliases
        queries: list[LeftJoinableSubquery]
            List of sub-queries to be left joined with table_expr

        Returns
        -------
        AggregationResult
        """
        aggregated_columns = []
        updated_table_expr = table_expr
        updated_query_index = current_query_index
        for internal_agg_result in queries:
            updated_table_expr = self._construct_left_join_sql(
                index=updated_query_index,
                table_expr=updated_table_expr,
                agg_result_names=internal_agg_result.column_names,
                join_keys=internal_agg_result.join_keys,
                agg_expr=internal_agg_result.expr,
            )
            updated_query_index += 1
            aggregated_columns.extend(internal_agg_result.column_names)

        result = AggregationResult(
            updated_table_expr=updated_table_expr,
            updated_index=updated_query_index,
            column_names=aggregated_columns,
        )
        return result

    @staticmethod
    def _construct_left_join_sql(
        index: int,
        agg_result_names: list[str],
        join_keys: list[str],
        table_expr: expressions.Select,
        agg_expr: expressions.Select,
    ) -> expressions.Select:
        """Construct SQL that left joins aggregated result back to request table

        Parameters
        ----------
        index : int
            Index of the current left join
        agg_result_names : list[str]
            Column names of the aggregated results
        join_keys : list[str]
            List of join keys
        table_expr : expressions.Select
            Table to which the left join should be added to
        agg_expr : expressions.Select
            SQL expression that performs the aggregation

        Returns
        -------
        Select
            Updated table expression
        """
        agg_table_alias = f"T{index}"
        agg_result_name_aliases = [
            alias_(
                get_qualified_column_identifier(agg_result_name, agg_table_alias, quote_table=True),
                agg_result_name,
                quoted=True,
            )
            for agg_result_name in agg_result_names
        ]
        join_conditions = [
            f"REQ.{quoted_identifier(key).sql()} = {agg_table_alias}.{quoted_identifier(key).sql()}"
            for key in join_keys
        ]
        updated_table_expr = table_expr.join(
            agg_expr.subquery(),
            join_type="left",
            join_alias=agg_table_alias,
            on=expressions.and_(*join_conditions),
        ).select(*agg_result_name_aliases)
        return updated_table_expr

    @staticmethod
    def _wrap_in_nested_query(table_expr: Select, columns: list[str]) -> Select:
        """
        Wrap table_expr in a nested query with a REQ alias

        This is so that subsequent joins using other aggregators can work. To be called by
        aggregators that perform aggregation using nested queries instead of using left joins.

        Parameters
        ----------
        table_expr: Select
            The table expression to be wrapped
        columns: list[str]
            Column names in the table

        Returns
        -------
        Select
        """
        wrapped_table_expr = select(
            *[
                alias_(get_qualified_column_identifier(col, "REQ"), col, quoted=True)
                for col in columns
            ]
        ).from_(table_expr.subquery(alias="REQ"))
        return wrapped_table_expr

    @abstractmethod
    def get_common_table_expressions(
        self, request_table_name: str
    ) -> list[tuple[str, expressions.Select]]:
        """
        Construct any common table expressions (CTE) required to support the aggregation, typically
        the original request table processed in some ways. This will be used to form the WITH
        section of the feature compute sql.

        Parameters
        ----------
        request_table_name: str
            Name of the request table

        Returns
        -------
        list[tuple[str, expressions.Select]]
            List of common table expressions as tuples. The first element of the tuple is the name
            of the CTE and the second element is the corresponding sql expression.
        """


class NonTileBasedAggregator(Aggregator[NonTileBasedAggregationSpecT], ABC):
    """
    Aggregators that do not use tiles
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.grouped_specs: dict[str, list[NonTileBasedAggregationSpecT]] = {}
        self.grouped_agg_result_names: dict[str, set[str]] = {}

    def update(self, aggregation_spec: NonTileBasedAggregationSpecT) -> None:
        """
        Update internal state to account for the given ItemAggregationSpec

        Parameters
        ----------
        aggregation_spec: NonTileBasedAggregationSpecT
            Aggregation specification
        """

        key = aggregation_spec.source_hash

        if key not in self.grouped_specs:
            self.grouped_agg_result_names[key] = set()
            self.grouped_specs[key] = []

        if aggregation_spec.agg_result_name in self.grouped_agg_result_names[key]:
            # Skip updating if the spec produces a result that was seen before.
            return

        self.grouped_agg_result_names[key].add(aggregation_spec.agg_result_name)
        self.grouped_specs[key].append(aggregation_spec)
