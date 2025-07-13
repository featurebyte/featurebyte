"""
Module for input data sql generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlglot import expressions
from sqlglot.expressions import Expression, Select

from featurebyte.enum import DBVarType, InternalName, TableDataType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.model.timestamp_schema import (
    TimestampSchema,
)
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    PartitionColumnFilter,
    get_fully_qualified_table_name,
    quoted_identifier,
)
from featurebyte.query_graph.sql.entity_filter import get_table_filtered_by_entity
from featurebyte.query_graph.sql.partition_filter import get_partition_filter
from featurebyte.query_graph.sql.partition_filter_helper import (
    get_default_partition_column_filter_buffer,
)
from featurebyte.query_graph.sql.timestamp_helper import convert_timestamp_to_utc


@dataclass
class InputNode(TableNode):
    """Input data node"""

    dbtable: dict[str, str]
    feature_store: dict[str, Any]
    query_node_type = NodeType.INPUT

    def from_query_impl(self, select_expr: Select) -> Select:
        dbtable = get_fully_qualified_table_name(self.dbtable)
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

            if push_down_filter.timestamp_schema is None:
                timestamp_col_expr = self.context.adapter.normalize_timestamp_before_comparison(
                    quoted_identifier(push_down_filter.timestamp_column_name)
                )
            else:
                timestamp_col_expr = convert_timestamp_to_utc(
                    quoted_identifier(push_down_filter.timestamp_column_name),
                    push_down_filter.timestamp_schema,
                    self.context.adapter,
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

            # Add filter on partition column with a buffer if available
            partition_column_filter = PartitionColumnFilter(
                from_timestamp=_maybe_cast(start_date_placeholder),
                to_timestamp=_maybe_cast(end_date_placeholder),
                buffer=get_default_partition_column_filter_buffer(),
            )
            select_expr = self._apply_partition_column_filter(
                select_expr=select_expr,
                columns=self.context.parameters["columns"],
                partition_column_filter=partition_column_filter,
                adapter=self.context.adapter,
            )

        return select_expr

    def _select_from_dbtable(self, select_expr: Select, dbtable: Expression) -> Select:
        select_expr = select_expr.from_(dbtable)

        # Apply partition column filters if available
        if self.context.partition_column_filters is not None:
            partition_column_filter = self.context.partition_column_filters.mapping.get(
                self.context.parameters["id"]
            )
        else:
            partition_column_filter = None
        if partition_column_filter:
            select_expr = self._apply_partition_column_filter(
                select_expr=select_expr,
                columns=self.context.parameters["columns"],
                partition_column_filter=partition_column_filter,
                adapter=self.context.adapter,
            )

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
                    input_expr=select_expr.select(*original_cols),
                    entity_column_names=entity_filter.entity_columns,
                    table_column_names=entity_filter.table_columns,
                    distinct=need_distinct,
                    adapter=self.context.adapter,
                ).subquery()
            )

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

        if column_spec.dtype == DBVarType.DATE:
            # Treat date type columns as end of day in the local timezone
            column_expr = adapter.dateadd_second(make_literal_value(86400), column_expr)

        return column_expr

    @classmethod
    def _apply_partition_column_filter(
        cls,
        select_expr: Select,
        columns: list[dict[str, Any]],
        partition_column_filter: PartitionColumnFilter,
        adapter: BaseAdapter,
    ) -> Select:
        partition_column = None
        dtype_metadata_mapping = {}
        for column_info in columns:
            partition_metadata = column_info.get("partition_metadata")
            if (
                partition_metadata is not None
                and partition_metadata.get("is_partition_key") is True
            ):
                partition_column = column_info["name"]
            dtype_metadata = column_info.get("dtype_metadata")
            if dtype_metadata is not None:
                dtype_metadata_mapping[column_info["name"]] = DBVarTypeMetadata(**dtype_metadata)

        format_string = None
        if partition_column is not None:
            dtype_metadata = dtype_metadata_mapping.get(partition_column)
            if dtype_metadata is not None and dtype_metadata.timestamp_schema is not None:
                format_string = dtype_metadata.timestamp_schema.format_string

        if partition_column is not None:
            partition_filter_condition = get_partition_filter(
                partition_column=partition_column,
                partition_column_filter=partition_column_filter,
                format_string=format_string,
                adapter=adapter,
            )
            select_expr = select_expr.where(partition_filter_condition)

        return select_expr
