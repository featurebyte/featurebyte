"""
Module for input data sql generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlglot import expressions
from sqlglot.expressions import Expression, Select

from featurebyte.enum import InternalName, TableDataType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.input import SampleParameters
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
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
            colname = col_dict["name"]
            columns_map[colname] = expressions.Identifier(this=colname, quoted=True)
        return columns_map
