"""
Module for input data sql generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic_extra_types.timezone_name import TimeZoneName
from sqlglot import expressions
from sqlglot.expressions import Expression, Select

from featurebyte.enum import DBVarType, InternalName, TableDataType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema, TimeZoneColumn
from featurebyte.query_graph.node.input import SampleParameters
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_fully_qualified_table_name, quoted_identifier
from featurebyte.query_graph.sql.entity_filter import get_table_filtered_by_entity


@dataclass
class InputNode(TableNode):
    """Input data node"""

    dbtable: dict[str, str]
    feature_store: dict[str, Any]
    query_node_type = NodeType.INPUT
    sample_parameters: SampleParameters | None = None

    def from_query_impl(self, select_expr: Select) -> Select:
        dbtable = get_fully_qualified_table_name(self.dbtable)
        if isinstance(self.sample_parameters, SampleParameters):
            original_cols = [
                quoted_identifier(col_info["name"])
                for col_info in self.context.parameters["columns"]
            ]
            sample_expr = self.context.adapter.random_sample(
                select_expr.from_(dbtable).select(*original_cols),
                desired_row_count=self.sample_parameters.num_rows,
                total_row_count=self.sample_parameters.total_num_rows,
                seed=self.sample_parameters.seed,
                sort_by_prob=self.sample_parameters.sort_by_prob,
            )
            select_expr = select_expr.from_(sample_expr.subquery())
        else:
            select_expr = self._select_from_dbtable(select_expr, dbtable)

        # Optionally, filter SCD table to only include current records. This is done only for
        # certain aggregations during online serving.
        if (
            self.context.parameters["type"] == TableDataType.SCD_TABLE
            and self.context.to_filter_scd_by_current_flag
        ):
            current_flag_column = self.context.parameters["current_flag_column"]
            if current_flag_column is not None:
                select_expr = select_expr.where(
                    expressions.EQ(
                        this=expressions.Identifier(this=current_flag_column, quoted=True),
                        expression=expressions.true(),
                    )
                )

        # When possible, push down timestamp filter to the input table. This is done only for event
        # table in scheduled tile tasks.
        push_down_filter = self.context.event_table_timestamp_filter
        if (
            push_down_filter is not None
            and self.context.parameters["id"] == push_down_filter.event_table_id
        ):
            if push_down_filter.start_timestamp_placeholder_name is None:
                start_date_placeholder = InternalName.TILE_START_DATE_SQL_PLACEHOLDER.value
            else:
                start_date_placeholder = push_down_filter.start_timestamp_placeholder_name
            if push_down_filter.end_timestamp_placeholder_name is None:
                end_date_placeholder = InternalName.TILE_END_DATE_SQL_PLACEHOLDER.value
            else:
                end_date_placeholder = push_down_filter.end_timestamp_placeholder_name

            def _maybe_cast(identifier_name: str) -> Expression:
                assert push_down_filter is not None
                if push_down_filter.to_cast_placeholders:
                    return expressions.Cast(
                        this=expressions.Identifier(this=identifier_name),
                        to=expressions.DataType.build("TIMESTAMP"),
                    )
                return expressions.Identifier(this=identifier_name)

            timestamp_col_expr = self.context.adapter.normalize_timestamp_before_comparison(
                quoted_identifier(push_down_filter.timestamp_column_name)
            )
            select_expr = select_expr.where(
                expressions.and_(
                    expressions.GTE(
                        this=timestamp_col_expr,
                        expression=_maybe_cast(start_date_placeholder),
                    ),
                    expressions.LT(
                        this=timestamp_col_expr,
                        expression=_maybe_cast(end_date_placeholder),
                    ),
                )
            )

        return select_expr

    def _select_from_dbtable(self, select_expr: Select, dbtable: Expression) -> Select:
        on_demand_entity_filters = self.context.on_demand_entity_filters
        if (
            on_demand_entity_filters is not None
            and self.context.parameters["id"] in on_demand_entity_filters.mapping
        ):
            original_cols = [
                quoted_identifier(col_info["name"])
                for col_info in self.context.parameters["columns"]
            ]
            entity_filter = on_demand_entity_filters.mapping[self.context.parameters["id"]]
            # Need to deduplicate entity table if entity_filter only uses a subset of the entity
            # columns
            need_distinct = len(entity_filter.entity_columns) != len(
                on_demand_entity_filters.entity_columns
            )
            select_expr = expressions.select().from_(
                get_table_filtered_by_entity(
                    input_expr=select_expr.select(*original_cols).from_(dbtable),
                    entity_column_names=entity_filter.entity_columns,
                    table_column_names=entity_filter.table_columns,
                    distinct=need_distinct,
                    adapter=self.context.adapter,
                ).subquery()
            )
        else:
            select_expr = select_expr.from_(dbtable)

        return select_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> InputNode | None:
        columns_map = cls.make_input_columns_map(context)
        feature_store = context.parameters["feature_store_details"]
        sql_node = InputNode(
            context=context,
            columns_map=columns_map,
            dbtable=context.parameters["table_details"],
            feature_store=feature_store,
            sample_parameters=context.query_node.parameters.sample_parameters,  # type: ignore
        )
        return sql_node

    @classmethod
    def make_input_columns_map(cls, context: SQLNodeContext) -> dict[str, Expression]:
        """
        Construct mapping from column name to expression

        Parameters
        ----------
        context : SQLNodeContext
            Context to build SQLNode

        Returns
        -------
        dict[str, Expression]
        """
        columns_map: dict[str, Expression] = {}
        for col_dict in context.parameters["columns"]:
            column_spec = ColumnSpec(**col_dict)
            colname = column_spec.name
            timestamp_schema = column_spec.timestamp_schema
            if (
                timestamp_schema is not None
                and context.parameters["type"] == TableDataType.SCD_TABLE
            ):
                column_expr = cls._get_generic_timestamp_column_expr(
                    column_spec, timestamp_schema, context.adapter
                )
            else:
                column_expr = expressions.Identifier(this=colname, quoted=True)
            columns_map[colname] = column_expr
        return columns_map

    @classmethod
    def _get_generic_timestamp_column_expr(
        cls,
        column_spec: ColumnSpec,
        timestamp_schema: TimestampSchema,
        adapter: BaseAdapter,
    ) -> Expression:
        """
        Get expression for a timestamp column converted to UTC

        Parameters
        ----------
        column_spec : ColumnSpec
            Column specification
        timestamp_schema : TimestampSchema
            Timestamp schema
        adapter : BaseAdapter
            SQL adapter

        Returns
        -------
        Expression
        """
        # Original datetime column (could be a timestamp, date or string)
        column_expr: Expression = expressions.Identifier(this=column_spec.name, quoted=True)

        # Convert to timestamp in local time
        if column_spec.dtype == DBVarType.VARCHAR:
            assert timestamp_schema.format_string is not None
            column_expr = adapter.to_timestamp_from_string(
                column_expr, timestamp_schema.format_string
            )
        elif column_spec.dtype == DBVarType.DATE:
            # Treat date type columns as end of day in the local timezone
            column_expr = adapter.dateadd_second(make_literal_value(86400), column_expr)

        if timestamp_schema.is_utc_time:
            # Already in UTC, nothing to do
            return column_expr

        # Convert to timestamp in UTC
        if timestamp_schema.timezone is not None:
            timezone_type: Literal["offset", "name"]
            if isinstance(timestamp_schema.timezone, TimeZoneName):
                timezone = make_literal_value(timestamp_schema.timezone)
                timezone_type = "name"
            else:
                assert isinstance(timestamp_schema.timezone, TimeZoneColumn)
                timezone = quoted_identifier(timestamp_schema.timezone.column_name)
                if timestamp_schema.timezone.type == "offset":
                    timezone_type = "offset"
                else:
                    timezone_type = "name"
            column_expr = adapter.convert_timezone_to_utc(column_expr, timezone, timezone_type)

        return column_expr
