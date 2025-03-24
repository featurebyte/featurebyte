"""
Models for tile compute query
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Hashable, List, Optional, cast

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, alias_, select

from featurebyte.enum import DBVarType, InternalName, SourceType
from featurebyte.models.tile_compute_query import (
    Prerequisite,
    PrerequisiteTable,
    QueryModel,
    TileComputeQuery,
)
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.sql.adapter import BaseAdapter, get_sql_adapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.entity_filter import get_table_filtered_by_entity
from featurebyte.query_graph.sql.groupby_helper import (
    GroupbyColumn,
    GroupbyKey,
    _split_agg_and_snowflake_vector_aggregation_columns,
)
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.tiling import TileSpec
from featurebyte.query_graph.sql.timestamp_helper import convert_timestamp_to_utc


@dataclass(frozen=True)
class TileTableInputColumn:
    """
    Represents a column in the input table for a tile compute query
    """

    name: str
    expr: Expression

    def get_signature(self, source_type: SourceType) -> Hashable:
        return self.name, sql_to_string(self.expr, source_type)


@dataclass
class TileComputeSpec:
    """
    Defines a tile compute query that supports further modifications, such as adding new tile
    columns while retaining the same source data and group-by structure, before being converted
    into a query string.
    """

    source_expr: Select  # input_node.sql without selecting any columns
    entity_table_expr: Optional[Expression]
    timestamp_column: TileTableInputColumn
    timestamp_metadata: Optional[DBVarTypeMetadata]
    key_columns: List[TileTableInputColumn]
    value_by_column: Optional[TileTableInputColumn]
    value_columns: List[TileTableInputColumn]
    tile_index_expr: Expression
    tile_column_specs: List[TileSpec]
    is_order_dependent: bool
    is_on_demand: bool
    source_info: SourceInfo

    # Internal names
    ROW_NUMBER = "__FB_ROW_NUMBER"

    @property
    def adapter(self) -> BaseAdapter:
        return get_sql_adapter(self.source_info)

    def get_tile_compute_query(self) -> TileComputeQuery:
        """
        Get the tile compute query object

        Returns
        -------
        """
        prerequisite_tables = []
        source_type = self.source_info.source_type
        entity_table_expr: Optional[Expression]
        if self.is_on_demand:
            if self.entity_table_expr is None:
                entity_table_expr = expressions.Identifier(
                    this=InternalName.ENTITY_TABLE_SQL_PLACEHOLDER
                )
            else:
                entity_table_expr = self.entity_table_expr
            prerequisite_tables.append(
                PrerequisiteTable(
                    name=InternalName.ENTITY_TABLE_NAME.value,
                    query=QueryModel.from_expr(entity_table_expr, source_type=source_type),
                )
            )
        input_filtered = self._get_input_filtered_within_date_range()
        prerequisite_tables.append(
            PrerequisiteTable(
                name=InternalName.TILE_COMPUTE_INPUT_TABLE_NAME.value,
                query=QueryModel.from_expr(input_filtered, source_type=source_type),
            )
        )
        return TileComputeQuery(
            prerequisite=Prerequisite(tables=prerequisite_tables),
            aggregation_query=QueryModel.from_expr(
                self._get_aggregated_tile_table(), source_type=source_type
            ),
        )

    def _get_tile_input_table(self) -> Select:
        input_columns = [self.timestamp_column] + self.key_columns
        if self.value_by_column is not None:
            input_columns.append(self.value_by_column)
        input_columns.extend(self.value_columns)
        required_cols = []
        for col in input_columns:
            required_cols.append(alias_(col.expr, alias=col.name, quoted=True))
        return self.source_expr.select(*required_cols)

    def _get_input_filtered_within_date_range(self) -> Select:
        """
        Construct sql to filter input data to be within a date range. Only data within this range
        will be used to calculate tiles.

        Returns
        -------
        Select
        """
        input_expr = self._get_tile_input_table()
        if self.is_on_demand:
            return self._get_input_filtered_within_date_range_on_demand(input_expr)
        return self._get_input_filtered_within_date_range_scheduled(input_expr)

    def _get_input_filtered_within_date_range_scheduled(self, input_expr: Select) -> Select:
        """
        Construct sql with placeholders for start and end dates to be filled in by the scheduled
        tile computation jobs

        Parameters
        ----------
        input_expr: Select
            Input table expression with all the required columns selected

        Returns
        -------
        Select
        """
        adapter = self.adapter
        select_expr = select("*").from_(input_expr.subquery())
        if (
            self.timestamp_metadata is not None
            and self.timestamp_metadata.timestamp_schema is not None
        ):
            timestamp_expr = convert_timestamp_to_utc(
                quoted_identifier(self.timestamp_column.name),
                self.timestamp_metadata.timestamp_schema,
                adapter,
            )
        else:
            timestamp_expr = adapter.normalize_timestamp_before_comparison(
                quoted_identifier(self.timestamp_column.name)
            )
        start_expr = expressions.Cast(
            this=expressions.Identifier(this=InternalName.TILE_START_DATE_SQL_PLACEHOLDER),
            to=expressions.DataType.build("TIMESTAMP"),
        )
        end_expr = expressions.Cast(
            this=expressions.Identifier(this=InternalName.TILE_END_DATE_SQL_PLACEHOLDER),
            to=expressions.DataType.build("TIMESTAMP"),
        )
        start_cond = expressions.GTE(this=timestamp_expr, expression=start_expr)
        end_cond = expressions.LT(this=timestamp_expr, expression=end_expr)
        select_expr = select_expr.where(start_cond, end_cond, copy=False)
        return select_expr

    def _get_input_filtered_within_date_range_on_demand(self, input_expr: Select) -> Select:
        """
        Construct sql to filter the data used when building tiles for selected entities only

        Parameters
        ----------
        input_expr: Select
            Input table expression with all the required columns selected

        Returns
        -------
        Select
        """
        return get_table_filtered_by_entity(
            input_expr=input_expr,
            entity_column_names=[col.name for col in self.key_columns],
            timestamp_column=self.timestamp_column.name,
            timestamp_metadata=self.timestamp_metadata,
            adapter=self.adapter,
        )

    def _get_aggregated_tile_table(self) -> Select:
        input_tiled = select("*", self.tile_index_expr).from_(
            InternalName.TILE_COMPUTE_INPUT_TABLE_NAME
        )
        keys = [quoted_identifier(col.name) for col in self.key_columns]
        if self.value_by_column is not None:
            keys.append(quoted_identifier(self.value_by_column.name))

        if self.is_order_dependent:
            return self._get_tile_sql_order_dependent(keys, input_tiled)

        return self._get_tile_sql_order_independent(keys, input_tiled)

    def _get_tile_sql_order_dependent(
        self,
        keys: list[Expression],
        input_tiled: expressions.Select,
    ) -> Select:
        def _make_window_expr(expr: str | Expression) -> Expression:
            order = expressions.Order(
                expressions=[
                    expressions.Ordered(
                        this=quoted_identifier(self.timestamp_column.name), desc=True
                    ),
                ]
            )
            partition_by = [cast(Expression, expressions.Identifier(this="index"))] + keys
            window_expr = expressions.Window(this=expr, partition_by=partition_by, order=order)
            return window_expr

        window_exprs = [
            alias_(
                _make_window_expr(expressions.Anonymous(this="ROW_NUMBER")),
                alias=self.ROW_NUMBER,
                quoted=True,
            ),
            *[
                alias_(_make_window_expr(spec.tile_expr), alias=spec.tile_column_name)
                for spec in self.tile_column_specs
            ],
        ]
        inner_expr = select(
            "index",
            *keys,
            *window_exprs,
        ).from_(input_tiled.subquery(copy=False), copy=False)

        outer_condition = expressions.EQ(
            this=quoted_identifier(self.ROW_NUMBER),
            expression=make_literal_value(1),
        )
        tile_expr = (
            select(
                "index",
                *keys,
                *[spec.tile_column_name for spec in self.tile_column_specs],
            )
            .from_(inner_expr.subquery(copy=False))
            .where(outer_condition, copy=False)
        )

        return tile_expr

    @staticmethod
    def _get_db_var_type_from_col_type(col_type: str) -> DBVarType:
        mapping: dict[str, DBVarType] = {"ARRAY": DBVarType.ARRAY, "EMBEDDING": DBVarType.EMBEDDING}
        return mapping.get(col_type, DBVarType.UNKNOWN)

    def _get_tile_sql_order_independent(
        self,
        keys: list[Expression],
        input_tiled: expressions.Select,
    ) -> Select:
        inner_groupby_keys = [expressions.Identifier(this="index"), *keys]
        groupby_keys = [GroupbyKey(expr=key, name=key.name) for key in inner_groupby_keys]
        original_query = select().from_(input_tiled.subquery(copy=False))

        groupby_columns = []
        for spec in self.tile_column_specs:
            groupby_columns.append(
                GroupbyColumn(
                    parent_dtype=self._get_db_var_type_from_col_type(spec.tile_column_type),
                    agg_func=spec.tile_aggregation_type,
                    parent_expr=spec.tile_expr,
                    result_name=spec.tile_column_name,
                    parent_cols=spec.tile_expr.expressions or [spec.tile_expr.alias_or_name],
                    quote_result_name=False,
                )
            )
        agg_exprs, vector_agg_exprs = _split_agg_and_snowflake_vector_aggregation_columns(
            original_query,
            groupby_keys,
            groupby_columns,
            None,
            self.adapter.source_type,
            is_tile=True,
        )

        return self.adapter.group_by(
            original_query,
            select_keys=inner_groupby_keys,
            agg_exprs=agg_exprs,
            keys=inner_groupby_keys,
            vector_aggregate_columns=vector_agg_exprs,
            quote_vector_agg_aliases=False,
        )
