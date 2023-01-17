"""
Module for input data sql generation
"""
from __future__ import annotations

from typing import Any

from dataclasses import dataclass

from sqlglot import expressions
from sqlglot.expressions import Expression, Select

from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import quoted_identifier


@dataclass
class InputNode(TableNode):
    """Input data node"""

    dbtable: dict[str, str]
    feature_store: dict[str, Any]
    query_node_type = NodeType.INPUT

    def from_query_impl(self, select_expr: Select) -> Select:
        dbtable: Expression
        if self.feature_store["type"] in {SourceType.SNOWFLAKE, SourceType.DATABRICKS}:
            database = self.dbtable["database_name"]
            schema = self.dbtable["schema_name"]
            table = self.dbtable["table_name"]
            # expressions.Table's notation for three part fully qualified name is
            # {catalog}.{db}.{this}
            dbtable = expressions.Table(
                this=quoted_identifier(table),
                db=quoted_identifier(schema),
                catalog=quoted_identifier(database),
            )
        else:
            dbtable = quoted_identifier(self.dbtable["table_name"])
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
