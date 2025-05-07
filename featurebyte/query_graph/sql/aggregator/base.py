"""
Base class for aggregation SQL generators
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Generic, Sequence, Tuple, TypeVar

from bson import ObjectId
from sqlglot import expressions
from sqlglot.expressions import Select, alias_, select

from featurebyte.enum import DBVarType, InternalName
from featurebyte.query_graph.sql.adapter import BaseAdapter, get_sql_adapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    CteStatement,
    get_qualified_column_identifier,
    quoted_identifier,
)
from featurebyte.query_graph.sql.online_serving_util import (
    get_online_store_table_name,
    get_version_placeholder,
)
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.specs import (
    AggregationSpec,
    NonTileBasedAggregationSpec,
    TileBasedAggregationSpec,
)

AggregationSpecT = TypeVar("AggregationSpecT", bound=AggregationSpec)
NonTileBasedAggregationSpecT = TypeVar(
    "NonTileBasedAggregationSpecT", bound=NonTileBasedAggregationSpec
)

LATEST_VERSION = "LATEST_VERSION"


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

    def get_expression_for_column(
        self, main_alias: str, join_alias: str, column_name: str, adapter: BaseAdapter
    ) -> expressions.Expression:
        """
        Get the expression for a column name after join. The default implementation is simply to
        return the qualified expression "{join_alias}"."{column_name}".

        Currently this method exists to allow lookup features from EventView to have a cutoff based
        on the main table's point in time column after the lookup join.

        Parameters
        ----------
        main_alias: str
            Alias of the left table
        join_alias: str
            Alias of the right table (which this LeftJoinableSubquery is representing)
        column_name: str
            Column name in the right table
        adapter: BaseAdapter
            SQL adapter

        Returns
        -------
        Expression
        """
        _ = main_alias
        _ = adapter
        return get_qualified_column_identifier(column_name, join_alias, quote_table=True)


@dataclass
class CommonTable:
    """
    Represents a common table expression (CTE) required for aggregation
    """

    name: str
    expr: expressions.Select
    quoted: bool = True
    should_materialize: bool = False

    def to_cte_statement(self) -> CteStatement:
        """
        Convert to a CTE statement

        Returns
        -------
        CteStatement
        """
        if self.quoted:
            return quoted_identifier(self.name), self.expr
        return self.name, self.expr


class Aggregator(Generic[AggregationSpecT], ABC):
    """
    Base class of all aggregators
    """

    def __init__(self, source_info: SourceInfo, is_online_serving: bool = False):
        self.source_info = source_info
        self.adapter = get_sql_adapter(source_info)
        self.is_online_serving = is_online_serving
        self.required_serving_names: set[str] = set()
        self.required_entity_ids: set[ObjectId] = set()

    def get_required_serving_names(self) -> set[str]:
        """
        Get the set of required serving names

        Returns
        -------
        set[str]
        """
        return self.required_serving_names.copy()

    def get_required_entity_ids(self) -> set[ObjectId]:
        """
        Get the set of required entity ids

        Returns
        -------
        set[ObjectId]
        """
        return self.required_entity_ids.copy()

    def update(self, aggregation_spec: AggregationSpecT) -> None:
        """
        Update internal states given an AggregationSpec

        Parameters
        ----------
        aggregation_spec: AggregationSpecT
            Aggregation specification
        """
        self.required_serving_names.update(aggregation_spec.serving_names)
        if aggregation_spec.entity_ids is not None:
            self.required_entity_ids.update(aggregation_spec.entity_ids)
        self.additional_update(aggregation_spec)

    @abstractmethod
    def additional_update(self, aggregation_spec: AggregationSpecT) -> None:
        """
        Additional updates to internal states specific to different aggregators. To be overridden by
        sub-classes

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
        queries: Sequence[LeftJoinableSubquery],
    ) -> AggregationResult:
        """
        Update table_expr by left joining with a list of LeftJoinableSubquery

        Parameters
        ----------
        table_expr: Select
            Table expression to update
        current_query_index: int
            A running integer to be used to construct new table aliases
        queries: Sequence[LeftJoinableSubquery]
            List of sub-queries to be left joined with table_expr

        Returns
        -------
        AggregationResult
        """
        aggregated_columns = []
        updated_table_expr = table_expr
        updated_query_index = current_query_index
        for subquery in queries:
            updated_table_expr = self._construct_left_join_sql(
                index=updated_query_index,
                table_expr=updated_table_expr,
                left_joinable_subquery=subquery,
            )
            updated_query_index += 1
            aggregated_columns.extend(subquery.column_names)

        result = AggregationResult(
            updated_table_expr=updated_table_expr,
            updated_index=updated_query_index,
            column_names=aggregated_columns,
        )
        return result

    def _construct_left_join_sql(
        self,
        index: int,
        table_expr: expressions.Select,
        left_joinable_subquery: LeftJoinableSubquery,
    ) -> expressions.Select:
        """Construct SQL that left joins aggregated result back to request table

        Parameters
        ----------
        index : int
            Index of the current left join
        table_expr : expressions.Select
            Table to which the left join should be added to
        left_joinable_subquery: LeftJoinableSubquery
            The subquery to be joined to table_expr

        Returns
        -------
        Select
            Updated table expression
        """
        agg_table_alias = f"T{index}"
        agg_result_name_aliases = [
            alias_(
                left_joinable_subquery.get_expression_for_column(
                    "REQ",
                    agg_table_alias,
                    agg_result_name,
                    adapter=self.adapter,
                ),
                agg_result_name,
                quoted=True,
            )
            for agg_result_name in left_joinable_subquery.column_names
        ]
        join_conditions = [
            f"REQ.{quoted_identifier(key).sql()} = {agg_table_alias}.{quoted_identifier(key).sql()}"
            for key in left_joinable_subquery.join_keys
        ]
        updated_table_expr = table_expr.join(
            left_joinable_subquery.expr.subquery(),
            join_type="left",
            join_alias=agg_table_alias,
            on=expressions.and_(*join_conditions) if join_conditions else expressions.true(),
            copy=False,
        ).select(*agg_result_name_aliases, copy=False)
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
        wrapped_table_expr = select(*[
            alias_(get_qualified_column_identifier(col, "REQ"), col, quoted=True) for col in columns
        ]).from_(table_expr.subquery(alias="REQ"))
        return wrapped_table_expr

    @abstractmethod
    def get_common_table_expressions(self, request_table_name: str) -> list[CommonTable]:
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
        list[CommonTable]
            List of common table expressions
        """


@dataclass
class TileBasedAggregatorOnlineJoinInfo:
    """
    Information required to perform a join with an online store table
    """

    # Serving names as stored in the online store table
    original_serving_names: list[str]

    # Serving names expected in the request table
    request_serving_names: list[str]

    # Data type of the online store table
    dtype: DBVarType

    # Mapping from original aggregation result name to serving names specific aggregation result
    # names
    agg_result_name_aliases: dict[str, str] = field(default_factory=dict)

    def get_original_agg_result_names(self) -> list[str]:
        """
        Get the list of aggregation result names as they would appear in the online store table

        Returns
        -------
        list[str]
        """
        return list(self.agg_result_name_aliases.keys())

    def get_mapped_agg_result_names(self) -> list[str]:
        """
        Get the list of aggregation result names in the final feature query

        Returns
        -------
        list[str]
        """
        return list(self.agg_result_name_aliases.values())


OnlineJoinInfoKeyType = Tuple[str, Tuple[str, ...]]


class TileBasedAggregator(Aggregator[TileBasedAggregationSpec], ABC):
    """
    Aggregator that handles TileBasedAggregationSpec
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.online_join_info: dict[OnlineJoinInfoKeyType, TileBasedAggregatorOnlineJoinInfo] = {}

    def update(self, aggregation_spec: TileBasedAggregationSpec) -> None:
        super().update(aggregation_spec)
        if self.is_online_serving:
            table_name = get_online_store_table_name(
                set(aggregation_spec.entity_ids if aggregation_spec.entity_ids is not None else []),
                self.adapter.get_physical_type_from_dtype(aggregation_spec.dtype),
            )
            request_serving_names = aggregation_spec.serving_names
            key = (table_name, tuple(request_serving_names))
            if key not in self.online_join_info:
                self.online_join_info[key] = TileBasedAggregatorOnlineJoinInfo(
                    original_serving_names=aggregation_spec.original_serving_names,
                    request_serving_names=request_serving_names,
                    dtype=aggregation_spec.dtype,
                )
            self.online_join_info[key].agg_result_name_aliases[
                aggregation_spec.original_agg_result_name
            ] = aggregation_spec.agg_result_name

    def update_aggregation_table_expr(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        if self.is_online_serving:
            return self.update_aggregation_table_expr_online(
                table_expr=table_expr,
                current_query_index=current_query_index,
            )
        return self.update_aggregation_table_expr_offline(
            table_expr=table_expr,
            point_in_time_column=point_in_time_column,
            current_columns=current_columns,
            current_query_index=current_query_index,
        )

    @abstractmethod
    def update_aggregation_table_expr_offline(
        self,
        table_expr: Select,
        point_in_time_column: str,
        current_columns: list[str],
        current_query_index: int,
    ) -> AggregationResult:
        """
        Compute aggregation result in offline mode

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

    def update_aggregation_table_expr_online(
        self,
        table_expr: Select,
        current_query_index: int,
    ) -> AggregationResult:
        """
        Lookup aggregation result from online store table in online mode

        Parameters
        ----------
        table_expr: Select
            The table expression to update. This is typically the temporary aggregation table that
            has the same number of rows as the request table, referred to in the final feature sql
            as _FB_AGGREGATED
        current_query_index: int
            A running integer to be used to construct new table aliases

        Returns
        -------
        AggregationResult
        """
        left_join_queries = []

        for (table_name, _), info in self.online_join_info.items():
            serving_names = self._alias_original_serving_names_to_request_serving_names(info)
            agg_result_names_exprs = [
                alias_(
                    quoted_identifier(original_name),
                    alias=transformed_name,
                    quoted=True,
                )
                for original_name, transformed_name in info.agg_result_name_aliases.items()
            ]
            pivoted_online_store = self._pivot_online_store_table(
                table_name=table_name,
                agg_result_names=list(info.get_original_agg_result_names()),
                serving_names=info.original_serving_names,
                dtype=info.dtype,
            )
            expr = select(
                *serving_names,
                *agg_result_names_exprs,
            ).from_(pivoted_online_store.subquery())
            query = LeftJoinableSubquery(
                expr,
                list(info.get_mapped_agg_result_names()),
                join_keys=info.request_serving_names,
            )
            left_join_queries.append(query)

        return self._update_with_left_joins(
            table_expr, current_query_index=current_query_index, queries=left_join_queries
        )

    def _pivot_online_store_table(
        self,
        table_name: str,
        agg_result_names: list[str],
        serving_names: list[str],
        dtype: DBVarType,
    ) -> Select:
        """
        Pivot the online store table from long to wide format. See an example below.

        Physical online store table's schema:

        CUST_ID  AGGREGATION_RESULT_NAME  VALUE
        ---------------------------------------
        C1       agg_id_1_30d             1.0
        C1       agg_id_1_90d             3.0
        C2       agg_id_1_30d             10.0
        C2       agg_id_1_90d             33.0
        C3       agg_id_1_90d             100.0

        Pivoted table's schema:

        CUST_ID  agg_id_1_30d  agg_id_1_90d
        -----------------------------------
        C1       1.0           3.0
        C2       10.0          33.0
        C3       NULL          100.0

        Parameters
        ----------
        table_name: str
            Online store table name
        agg_result_names: list[str]
            Name of the aggregation results
        serving_names: list[str]
            Serving names
        dtype: DBVarType
            Data type of the aggregation results

        Returns
        -------
        Select
        """
        literal_agg_result_names = [make_literal_value(name) for name in agg_result_names]
        value_column = self.adapter.online_store_pivot_prepare_value_column(dtype)

        # Get the latest version of the online store table
        values_expressions = [
            expressions.Tuple(
                expressions=[
                    make_literal_value(agg_result_name),
                    expressions.Identifier(this=get_version_placeholder(agg_result_name)),
                ]
            )
            for agg_result_name in agg_result_names
        ]
        values_alias = expressions.TableAlias(
            this=expressions.Identifier(this="version_table"),
            columns=[
                quoted_identifier(InternalName.ONLINE_STORE_RESULT_NAME_COLUMN),
                quoted_identifier(LATEST_VERSION),
            ],
        )
        latest_result_version = select(
            quoted_identifier(InternalName.ONLINE_STORE_RESULT_NAME_COLUMN),
            quoted_identifier(LATEST_VERSION),
        ).from_(expressions.Values(expressions=values_expressions, alias=values_alias))
        latest_online_store_table = (
            select("R.*")
            .from_(latest_result_version.subquery(alias="L"))
            .join(
                table_name,
                join_alias="R",
                join_type="inner",
                on=expressions.and_(
                    expressions.EQ(
                        this=get_qualified_column_identifier(
                            InternalName.ONLINE_STORE_RESULT_NAME_COLUMN, "R"
                        ),
                        expression=get_qualified_column_identifier(
                            InternalName.ONLINE_STORE_RESULT_NAME_COLUMN, "L"
                        ),
                    ),
                    expressions.EQ(
                        this=get_qualified_column_identifier(
                            InternalName.ONLINE_STORE_VERSION_COLUMN, "R"
                        ),
                        expression=get_qualified_column_identifier(LATEST_VERSION, "L"),
                    ),
                ),
            )
        )

        # Filter the latest version of the online store table to only include the required
        # aggregation results
        filtered_online_store = (
            select(
                *[quoted_identifier(serving_name) for serving_name in serving_names],
                quoted_identifier(InternalName.ONLINE_STORE_RESULT_NAME_COLUMN),
                value_column,
            )
            .from_(latest_online_store_table.subquery())
            .where(
                expressions.In(
                    this=quoted_identifier(InternalName.ONLINE_STORE_RESULT_NAME_COLUMN),
                    expressions=literal_agg_result_names,
                )
            )
        )
        pivots = [
            expressions.Pivot(
                expressions=[
                    self.adapter.online_store_pivot_aggregation_func(
                        quoted_identifier(InternalName.ONLINE_STORE_VALUE_COLUMN.value)
                    )
                ],
                field=expressions.In(
                    this=quoted_identifier(InternalName.ONLINE_STORE_RESULT_NAME_COLUMN),
                    expressions=literal_agg_result_names,
                ),
            )
        ]
        pivot_subquery = expressions.Subquery(this=filtered_online_store, pivots=pivots)
        pivot_result = select(
            *[
                self.adapter.online_store_pivot_finalise_serving_name(serving_name)
                for serving_name in serving_names
            ],
            *[
                expressions.Alias(
                    this=self.adapter.online_store_pivot_finalise_value_column(
                        agg_result_name, dtype
                    ),
                    alias=quoted_identifier(agg_result_name),
                )
                for agg_result_name in agg_result_names
            ],
        ).from_(pivot_subquery)
        return pivot_result

    def _alias_original_serving_names_to_request_serving_names(
        self, info: TileBasedAggregatorOnlineJoinInfo
    ) -> list[expressions.Expression]:
        out = []
        for original_serving_name, request_serving_name in zip(
            info.original_serving_names, info.request_serving_names
        ):
            out.append(
                alias_(
                    quoted_identifier(original_serving_name),
                    alias=request_serving_name,
                    quoted=True,
                )
            )
        return out


class NonTileBasedAggregator(Aggregator[NonTileBasedAggregationSpecT], ABC):
    """
    Inherited by Aggregators that do not use tiles. Responsible for grouping aggregations that can
    be performed in the same subquery.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.grouped_specs: dict[str, list[NonTileBasedAggregationSpecT]] = {}
        self.grouped_agg_result_names: dict[str, set[str]] = {}

    def update(self, aggregation_spec: NonTileBasedAggregationSpecT) -> None:
        """
        Update internal states to account for aggregation_spec

        Parameters
        ----------
        aggregation_spec: NonTileBasedAggregationSpecT
            Aggregation specification
        """
        super().update(aggregation_spec)

        key = aggregation_spec.source_hash

        if key not in self.grouped_specs:
            self.grouped_agg_result_names[key] = set()
            self.grouped_specs[key] = []

        if aggregation_spec.agg_result_name in self.grouped_agg_result_names[key]:
            # Skip updating if the spec produces a result that was seen before.
            return

        self.grouped_agg_result_names[key].add(aggregation_spec.agg_result_name)
        self.grouped_specs[key].append(aggregation_spec)
