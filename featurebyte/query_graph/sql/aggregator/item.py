"""
SQL generation for aggregation without time windows from ItemView
"""
from __future__ import annotations

from typing import Any, cast

from sqlglot import expressions
from sqlglot.expressions import Select, select

from featurebyte.query_graph.sql.aggregator.base import (
    AggregationResult,
    Aggregator,
    LeftJoinableSubquery,
)
from featurebyte.query_graph.sql.aggregator.window import AggSpecEntityIDs
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.specs import ItemAggregationSpec


class NonTimeAwareRequestTablePlan:
    """SQL generation for request table in non-time aware aggregations

    In non-time aware aggregations such as ItemData groupby, the feature value can be determined by
    entity columns. The point in time is irrelevant.
    """

    def __init__(self) -> None:
        self.request_table_names: dict[AggSpecEntityIDs, str] = {}
        self.agg_specs: dict[AggSpecEntityIDs, ItemAggregationSpec] = {}

    def add_aggregation_spec(self, agg_spec: ItemAggregationSpec) -> None:
        """
        Update state given an ItemAggregationSpec

        Parameters
        ----------
        agg_spec: ItemAggregationSpec
            ItemAggregationSpec object
        """
        key = self.get_key(agg_spec)
        request_table_name = f"REQUEST_TABLE_{'_'.join(agg_spec.serving_names)}"
        self.request_table_names[key] = request_table_name
        self.agg_specs[key] = agg_spec

    def get_request_table_name(self, agg_spec: ItemAggregationSpec) -> str:
        """
        Get the processed request table name corresponding to an aggregation spec

        Parameters
        ----------
        agg_spec: ItemAggregationSpec
            ItemAggregationSpec object

        Returns
        -------
        str
        """
        key = self.get_key(agg_spec)
        return self.request_table_names[key]

    @staticmethod
    def get_key(agg_spec: ItemAggregationSpec) -> AggSpecEntityIDs:
        """
        Get an internal key used to determine request table sharing

        Parameters
        ----------
        agg_spec: ItemAggregationSpec
            ItemAggregationSpec object

        Returns
        -------
        AggSpecEntityIDs
        """
        return tuple(agg_spec.serving_names)

    def construct_request_table_ctes(
        self,
        request_table_name: str,
    ) -> list[tuple[str, expressions.Select]]:
        """
        Construct SQL statements that build the processed request tables

        Parameters
        ----------
        request_table_name : str
            Name of request table to use

        Returns
        -------
        list[tuple[str, expressions.Select]]
        """
        ctes = []
        for key, table_name in self.request_table_names.items():
            agg_spec = self.agg_specs[key]
            request_table_expr = self.construct_request_table_expr(agg_spec, request_table_name)
            ctes.append((quoted_identifier(table_name).sql(), request_table_expr))
        return ctes

    @staticmethod
    def construct_request_table_expr(
        agg_spec: ItemAggregationSpec,
        request_table_name: str,
    ) -> expressions.Select:
        """
        Construct a Select statement that forms the processed request table

        Parameters
        ----------
        agg_spec: ItemAggregationSpec
            ItemAggregationSpec object
        request_table_name: str
            Name of the original request table that is determined at runtime

        Returns
        -------
        expressions.Select
        """
        quoted_serving_names = [quoted_identifier(x) for x in agg_spec.serving_names]
        select_distinct_expr = select(*quoted_serving_names).distinct().from_(request_table_name)
        return select_distinct_expr


class ItemAggregator(Aggregator):
    """
    ItemAggregator is responsible for SQL generation for aggregation without time windows from
    ItemView
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.item_aggregation_specs: list[ItemAggregationSpec] = []
        self.non_time_aware_request_table_plan: NonTimeAwareRequestTablePlan = (
            NonTimeAwareRequestTablePlan()
        )

    def get_required_serving_names(self) -> set[str]:
        """
        Get the set of required serving names

        Returns
        -------
        set[str]
        """
        out = set()
        for agg_spec in self.item_aggregation_specs:
            out.update(agg_spec.serving_names)
        return out

    def update(self, aggregation_spec: ItemAggregationSpec) -> None:
        """
        Update internal state to account for the given ItemAggregationSpec

        Parameters
        ----------
        aggregation_spec: ItemAggregationSpec
            Aggregation specification
        """
        self.item_aggregation_specs.append(aggregation_spec)
        self.non_time_aware_request_table_plan.add_aggregation_spec(aggregation_spec)

    def construct_item_aggregation_sql(self, agg_spec: ItemAggregationSpec) -> expressions.Select:
        """
        Construct SQL for non-time aware item aggregation

        The required item groupby statement is contained in the ItemAggregationSpec object. This
        simply needs to perform an inner join between the corresponding request table with the item
        aggregation subquery.

        Parameters
        ----------
        agg_spec: ItemAggregationSpec
            ItemAggregationSpec object

        Returns
        -------
        expressions.Select
        """
        join_conditions_lst = []
        select_cols = [
            f"ITEM_AGG.{quoted_identifier(agg_spec.feature_name).sql()}"
            f" AS {quoted_identifier(agg_spec.agg_result_name).sql()}"
        ]
        for serving_name, key in zip(agg_spec.serving_names, agg_spec.keys):
            serving_name = quoted_identifier(serving_name).sql()
            key = quoted_identifier(key).sql()
            join_conditions_lst.append(f"REQ.{serving_name} = ITEM_AGG.{key}")
            select_cols.append(f"REQ.{serving_name} AS {serving_name}")

        request_table_name = self.non_time_aware_request_table_plan.get_request_table_name(agg_spec)
        item_agg_expr = cast(expressions.Select, agg_spec.agg_expr)
        agg_expr = (
            select(*select_cols)
            .from_(expressions.alias_(quoted_identifier(request_table_name), alias="REQ"))
            .join(
                item_agg_expr.subquery(),
                join_type="inner",
                join_alias="ITEM_AGG",
                on=expressions.and_(*join_conditions_lst),
            )
        )
        return agg_expr

    def get_item_aggregations(self) -> list[LeftJoinableSubquery]:
        """
        Get item aggregation queries

        Returns
        -------
        list[LeftJoinableSubquery]
        """
        results = []
        for item_agg_spec in self.item_aggregation_specs:
            agg_expr = self.construct_item_aggregation_sql(item_agg_spec)
            result = LeftJoinableSubquery(
                expr=agg_expr,
                column_names=[item_agg_spec.feature_name],
                join_keys=item_agg_spec.serving_names,
            )
            results.append(result)
        return results

    def update_aggregation_table_expr(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:

        _ = point_in_time_column
        queries = self.get_item_aggregations()

        return self._update_with_left_joins(
            table_expr=table_expr, current_query_index=current_query_index, queries=queries
        )

    def get_common_table_expressions(
        self, request_table_name: str
    ) -> list[tuple[str, expressions.Select]]:
        return self.non_time_aware_request_table_plan.construct_request_table_ctes(
            request_table_name
        )
