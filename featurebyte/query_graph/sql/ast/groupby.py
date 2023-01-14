"""
Module for groupby operation (non-time aware) sql generation
"""
from __future__ import annotations

from typing import cast

from dataclasses import dataclass

from sqlglot.expressions import Expression, select

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import SQLType, quoted_identifier
from featurebyte.query_graph.sql.groupby_helper import GroupbyColumn, get_groupby_expr


@dataclass
class ItemGroupby(TableNode):
    """
    ItemGroupby SQLNode
    """

    input_node: TableNode
    keys: list[str]
    groupby_columns: list[GroupbyColumn]
    query_node_type = NodeType.ITEM_GROUPBY

    @property
    def sql(self) -> Expression:
        quoted_keys = [quoted_identifier(k) for k in self.keys]
        return get_groupby_expr(
            input_expr=select().from_(self.input_node.sql_nested()),
            groupby_keys=quoted_keys,
            groupby_columns=self.groupby_columns,
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
        groupby_columns = [
            GroupbyColumn(
                agg_func=parameters["agg_func"],
                parent=parameters["parent"],
                result_name=output_name,
            )
        ]
        node = ItemGroupby(
            context=context,
            columns_map=columns_map,
            input_node=cast(TableNode, context.input_sql_nodes[0]),
            keys=parameters["keys"],
            groupby_columns=groupby_columns,
        )
        return node
