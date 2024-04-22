"""
Module for input data sql generation
"""

from __future__ import annotations

from typing import Any

from dataclasses import dataclass

from sqlglot import expressions
from sqlglot.expressions import Expression, Select

from featurebyte.enum import InternalName, TableDataType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import get_fully_qualified_table_name, quoted_identifier


@dataclass
class InputNode(TableNode):
    """Input data node"""

    dbtable: dict[str, str]
    feature_store: dict[str, Any]
    query_node_type = NodeType.INPUT

    def from_query_impl(self, select_expr: Select) -> Select:
        dbtable = get_fully_qualified_table_name(self.dbtable)
        select_expr = select_expr.from_(dbtable)

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

            select_expr = select_expr.where(
                expressions.and_(
                    expressions.GTE(
                        this=quoted_identifier(push_down_filter.timestamp_column_name),
                        expression=_maybe_cast(start_date_placeholder),
                    ),
                    expressions.LT(
                        this=quoted_identifier(push_down_filter.timestamp_column_name),
                        expression=_maybe_cast(end_date_placeholder),
                    ),
                )
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
            colname = col_dict["name"]
            columns_map[colname] = expressions.Identifier(this=colname, quoted=True)
        return columns_map
