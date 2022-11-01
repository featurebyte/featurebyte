"""
Module for groupby operation (non-time aware) sql generation
"""
from __future__ import annotations

from typing import cast

from dataclasses import dataclass

from sqlglot import Expression, expressions, parse_one, select

from featurebyte.enum import AggFunc
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import quoted_identifier


@dataclass
class ItemGroupby(TableNode):
    """
    ItemGroupby SQLNode
    """

    input_node: TableNode
    keys: list[str]
    agg_expr: Expression
    output_name: str
    query_node_type = NodeType.ITEM_GROUPBY

    @property
    def sql(self) -> Expression:
        quoted_keys = [quoted_identifier(k) for k in self.keys]
        expr = (
            select(
                *quoted_keys, expressions.alias_(self.agg_expr, quoted_identifier(self.output_name))
            )
            .from_(self.input_node.sql_nested())
            .group_by(*quoted_keys)
        )
        return expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> ItemGroupby:
        parameters = context.parameters
        agg_expr = cls.get_agg_expr(
            agg_func=parameters["agg_func"], input_column=parameters["parent"]
        )
        columns_map = {}
        for key in parameters["keys"]:
            columns_map[key] = quoted_identifier(key)
        output_name = parameters["names"][0]
        columns_map[output_name] = quoted_identifier(output_name)
        node = ItemGroupby(
            context=context,
            columns_map=columns_map,
            input_node=cast(TableNode, context.input_sql_nodes[0]),
            keys=parameters["keys"],
            agg_expr=agg_expr,
            output_name=output_name,
        )
        return node

    @classmethod
    def get_agg_expr(cls, agg_func: AggFunc, input_column: str) -> Expression:
        """
        Convert an AggFunc and input column name to a SQL expression to be used in GROUP BY

        Parameters
        ----------
        agg_func : AggFunc
            Aggregation function
        input_column : str
            Input column name

        Returns
        -------
        Expression
        """
        agg_func_sql_mapping = {
            AggFunc.SUM: "SUM",
            AggFunc.AVG: "AVG",
            AggFunc.MIN: "MIN",
            AggFunc.MAX: "MAX",
            AggFunc.STD: "STDDEV",
        }
        if agg_func in agg_func_sql_mapping:
            assert input_column is not None
            sql_func = agg_func_sql_mapping[agg_func]
            expr = expressions.Anonymous(
                this=sql_func, expressions=[quoted_identifier(input_column)]
            )
        else:
            if agg_func == AggFunc.COUNT:
                expr = parse_one("COUNT(*)")
            else:
                # Must be NA_COUNT
                assert agg_func == AggFunc.NA_COUNT
                assert input_column is not None
                expr_is_null = expressions.Is(
                    this=quoted_identifier(input_column), expression=expressions.NULL
                )
                expr = expressions.Sum(
                    this=expressions.Cast(this=expr_is_null, to=parse_one("INTEGER"))
                )
        return expr
