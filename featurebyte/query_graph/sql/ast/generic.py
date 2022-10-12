"""
Module for generic operations sql generation
"""
from __future__ import annotations

from typing import Any, Optional

from dataclasses import dataclass

from sqlglot import Expression, expressions, parse_one, select

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.sql.ast.base import (
    ExpressionNode,
    SQLNode,
    SQLNodeContext,
    TableNode,
    make_literal_value,
)
from featurebyte.query_graph.sql.ast.input import InputNode
from featurebyte.query_graph.sql.common import quoted_identifier


@dataclass
class StrExpressionNode(ExpressionNode):
    """Expression node created from string"""

    expr: str

    @property
    def sql(self) -> Expression:
        return parse_one(self.expr)


@dataclass
class ParsedExpressionNode(ExpressionNode):
    """Expression node"""

    expr: Expression

    @property
    def sql(self) -> Expression:
        return self.expr


@dataclass
class Project(ExpressionNode):
    """Project node for a single column"""

    column_name: str

    @property
    def sql(self) -> Expression:
        return self.table_node.get_column_expr(self.column_name)

    @property
    def sql_standalone(self) -> Expression:
        # This is overridden to bypass self.sql - the column expression would have been evaluated in
        # self.table_node.sql_nested already, and the expression must not be evaluated again.
        # Instead, simply select the column name from the nested query.
        return select(quoted_identifier(self.column_name)).from_(self.table_node.sql_nested())


@dataclass
class AliasNode(ExpressionNode):
    """Alias node that represents assignment to FeatureGroup"""

    name: str
    expr_node: ExpressionNode
    query_node_type = NodeType.ALIAS

    @property
    def sql(self) -> Expression:
        return self.expr_node.sql

    @classmethod
    def build(cls, context: SQLNodeContext) -> AliasNode:
        expr_node = context.input_sql_nodes[0]
        assert isinstance(expr_node, ExpressionNode)
        sql_node = AliasNode(
            table_node=expr_node.table_node, name=context.parameters["name"], expr_node=expr_node
        )
        return sql_node


@dataclass
class Conditional(ExpressionNode):
    """Conditional node"""

    series_node: ExpressionNode
    mask: ExpressionNode
    value: Any
    query_node_type = NodeType.CONDITIONAL

    @property
    def sql(self) -> Expression:
        if_expr = expressions.If(this=self.mask.sql, true=make_literal_value(self.value))
        expr = expressions.Case(ifs=[if_expr], default=self.series_node.sql)
        return expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> Conditional:
        input_sql_nodes = context.input_sql_nodes
        assert len(input_sql_nodes) == 2

        series_node = input_sql_nodes[0]
        mask = input_sql_nodes[1]
        value = context.parameters["value"]
        assert isinstance(series_node, ExpressionNode)
        assert isinstance(mask, ExpressionNode)
        input_table_node = series_node.table_node

        sql_node = Conditional(
            table_node=input_table_node, series_node=series_node, mask=mask, value=value
        )
        return sql_node


def make_project_node(
    input_sql_nodes: list[SQLNode],
    parameters: dict[str, Any],
    output_type: NodeOutputType,
) -> Project | TableNode:
    """Create a Project or ProjectMulti node

    Parameters
    ----------
    input_sql_nodes : list[SQLNode]
        List of input SQL nodes
    parameters : dict[str, Any]
        Query node parameters
    output_type : NodeOutputType
        Query node output type

    Returns
    -------
    Project | TableNode
        The appropriate SQL node for projection
    """
    table_node = input_sql_nodes[0]
    assert isinstance(table_node, TableNode)
    columns = parameters["columns"]
    sql_node: Project | TableNode
    if output_type == NodeOutputType.SERIES:
        sql_node = Project(table_node=table_node, column_name=columns[0])
    else:
        sql_node = table_node.subset_columns(columns)
    return sql_node


def make_assign_node(input_sql_nodes: list[SQLNode], parameters: dict[str, Any]) -> TableNode:
    """
    Create a TableNode for an assign operation

    Parameters
    ----------
    input_sql_nodes : list[SQLNode]
        List of input SQL nodes
    parameters : dict[str, Any]
        Query graph node parameters

    Returns
    -------
    TableNode
    """
    input_table_node = input_sql_nodes[0]
    assert isinstance(input_table_node, TableNode)
    if len(input_sql_nodes) == 2:
        expr_node = input_sql_nodes[1]
    else:
        expr_node = ParsedExpressionNode(input_table_node, make_literal_value(parameters["value"]))
    assert isinstance(expr_node, ExpressionNode)
    sql_node = input_table_node.copy()
    sql_node.assign_column(parameters["name"], expr_node)
    return sql_node


def resolve_project_node(expr_node: ExpressionNode) -> Optional[ExpressionNode]:
    """Resolves a Project node to the original ExpressionNode due to assignment

    This is needed when we need additional information tied to original ExpressionNode than just the
    constructed sql expression. Such information is lost in a Project node.

    Parameters
    ----------
    expr_node : ExpressionNode
        The ExpressionNode to resolve if it is a Project node

    Returns
    -------
    Optional[ExpressionNode]
        The original ExpressionNode or None if the node is never assigned (e.g. original columns
        that exist in the input table)
    """
    if not isinstance(expr_node, Project):
        return expr_node
    table_node = expr_node.table_node
    assigned_node = table_node.get_column_node(expr_node.column_name)
    return assigned_node


def handle_filter_node(
    input_sql_nodes: list[SQLNode], output_type: NodeOutputType
) -> TableNode | ExpressionNode:
    """Create a TableNode or ExpressionNode with filter condition

    Parameters
    ----------
    input_sql_nodes : list[SQLNode]
        List of input SQL nodes
    output_type : NodeOutputType
        Query node output type

    Returns
    -------
    TableNode | ExpressionNode
        The appropriate SQL node for the filtered result
    """
    item, mask = input_sql_nodes
    assert isinstance(mask, ExpressionNode)
    sql_node: TableNode | ExpressionNode
    if output_type == NodeOutputType.FRAME:
        assert isinstance(item, InputNode)
        sql_node = item.subset_rows(mask.sql)
    else:
        assert isinstance(item, ExpressionNode)
        assert isinstance(item.table_node, InputNode)
        input_table_copy = item.table_node.subset_rows(mask.sql)
        sql_node = ParsedExpressionNode(input_table_copy, item.sql)
    return sql_node
