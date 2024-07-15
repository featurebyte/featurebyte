"""
Module for groupby operation (non-time aware) sql generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from sqlglot.expressions import Expression, select

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import SQLType, quoted_identifier
from featurebyte.query_graph.sql.groupby_helper import GroupbyColumn, GroupbyKey, get_groupby_expr
from featurebyte.query_graph.sql.query_graph_util import get_parent_dtype


@dataclass
class ItemGroupby(TableNode):
    """
    ItemGroupby SQLNode
    """

    input_node: TableNode
    keys: list[str]
    value_by: str
    groupby_columns: list[GroupbyColumn]
    query_node_type = NodeType.ITEM_GROUPBY

    @property
    def sql(self) -> Expression:
        groupby_keys = [GroupbyKey(expr=quoted_identifier(k), name=k) for k in self.keys]
        value_by = (
            GroupbyKey(expr=quoted_identifier(self.value_by), name=self.value_by)
            if self.value_by
            else None
        )
        return get_groupby_expr(
            input_expr=select().from_(self.input_node.sql_nested()),
            groupby_keys=groupby_keys,
            groupby_columns=self.groupby_columns,
            value_by=value_by,
            adapter=self.context.adapter,
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> ItemGroupby | None:
        if context.sql_type in {SQLType.AGGREGATION, SQLType.POST_AGGREGATION}:
            return None
        parameters = context.parameters
        columns_map = {}
        for key in parameters["keys"]:
            columns_map[key] = quoted_identifier(key)
        output_name = parameters["name"]
        columns_map[output_name] = quoted_identifier(output_name)
        parent_dtype = None
        if parameters["parent"]:
            parent_dtype = get_parent_dtype(parameters["parent"], context.graph, context.query_node)
        groupby_columns = [
            GroupbyColumn(
                agg_func=parameters["agg_func"],
                parent_expr=(
                    quoted_identifier(parameters["parent"]) if parameters["parent"] else None
                ),
                result_name=output_name,
                parent_dtype=parent_dtype,
                parent_cols=(
                    [quoted_identifier(parameters["parent"])] if parameters["parent"] else []
                ),
            )
        ]
        node = ItemGroupby(
            context=context,
            columns_map=columns_map,
            input_node=cast(TableNode, context.input_sql_nodes[0]),
            keys=parameters["keys"],
            value_by=parameters["value_by"],
            groupby_columns=groupby_columns,
        )
        return node
