"""
This module contains the list of SQL operations to be used by the Query Graph Interpreter
"""
from typing import Any, Dict, List, Type, Union

# pylint: disable=W0511 (fixme)
# pylint: disable=R0903 (too-few-public-methods)
from abc import ABC, abstractmethod
from dataclasses import dataclass

from sqlglot import Expression, expressions, parse_one, select

from featurebyte.query_graph.enum import NodeType


class SQLNode(ABC):
    """Base class of a node in the SQL operations tree

    Query Graph Interpreter constructs a tree that represents the list of SQL operations required to
    produce the feature described by the Query Graph. Each SQL operation can be represented as a
    node in this tree. This is the interface that a node in this tree should implement.
    """

    @property
    @abstractmethod
    def sql(self) -> Union[Expression, expressions.Subqueryable]:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """


class TableNode(SQLNode, ABC):
    """Nodes that produce table-like output that can be used as nested input"""

    @property
    @abstractmethod
    def columns(self) -> List[str]:
        """Columns that are available in this table

        Returns
        -------
        List[str]
            List of column names
        """

    def sql_nested(self) -> Expression:
        """SQL expression that can be used within from_() to form a nested query

        Returns
        -------
        Expression
            Expression that can be used within from_()
        """
        sql = self.sql
        assert isinstance(sql, expressions.Subqueryable)
        return sql.subquery()


@dataclass
class StrExpressionNode(SQLNode):
    """Expression node created from string"""

    expr: str

    @property
    def sql(self) -> Expression:
        return parse_one(self.expr)


@dataclass
class ExpressionNode(SQLNode):
    """Expression node"""

    expr: Expression

    @property
    def sql(self) -> Expression:
        return self.expr


@dataclass
class BuildTileInputNode(TableNode):
    """Input data node used when building tiles"""

    column_names: List[str]
    timestamp: str
    input: StrExpressionNode

    @property
    def columns(self) -> List[str]:
        return self.column_names

    @property
    def sql(self) -> Expression:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """
        select_expr = select(*self.columns)
        select_expr = select_expr.from_(self.input.sql)
        select_expr = select_expr.where(
            f"{self.timestamp} >= CAST(FBT_START_DATE AS TIMESTAMP)",
            f"{self.timestamp} < CAST(FBT_END_DATE AS TIMESTAMP)",
        )
        return select_expr


@dataclass
class BinaryOp(SQLNode):
    """Binary operation node"""

    left: SQLNode
    right: SQLNode
    operation: Type[expressions.Expression]

    @property
    def sql(self) -> Expression:
        return self.operation(this=self.left.sql, expression=self.right.sql)


@dataclass
class Project(SQLNode):
    """Project node"""

    columns: List[str]

    @property
    def sql(self) -> Expression:
        assert len(self.columns) == 1
        return parse_one(self.columns[0])


@dataclass
class AssignNode(TableNode):
    """Assign node"""

    table: TableNode
    column: SQLNode
    name: str

    @property
    def columns(self) -> List[str]:
        return [x for x in self.table.columns if x != self.name] + [self.name]

    @property
    def sql(self) -> Expression:
        existing_columns = [col for col in self.table.columns if col != self.name]
        select_expr = select(*existing_columns)
        select_expr = select_expr.select(expressions.alias_(self.column.sql, self.name))
        select_expr = select_expr.from_(self.table.sql_nested())
        return select_expr


@dataclass
class BuildTileNode(TableNode):
    """Tile builder node

    This node is responsible for generating the tile building SQL for a groupby operation.
    """

    input: TableNode
    key: str
    parent: str
    timestamp: str
    agg_func: str
    frequency: int

    @property
    def columns(self) -> List[str]:
        return ["tile_start_date", self.key, "value"]

    @property
    def sql(self) -> Expression:
        start_date_placeholder = "FBT_START_DATE"
        start_date_placeholder_epoch = (
            f"DATE_PART(EPOCH_SECOND, CAST({start_date_placeholder} AS TIMESTAMP))"
        )
        timestamp_epoch = f"DATE_PART(EPOCH_SECOND, {self.timestamp})"

        input_tiled = select(
            "*",
            f"FLOOR(({timestamp_epoch} - {start_date_placeholder_epoch}) / {self.frequency}) AS tile_index",
        ).from_(self.input.sql_nested())

        tile_start_date = (
            f"TO_TIMESTAMP({start_date_placeholder_epoch} + tile_index * {self.frequency})"
        )
        groupby_sql = (
            select(
                f"{tile_start_date} AS tile_start_date",
                self.key,
                f"{self.agg_func}({self.parent}) AS value",
            )
            .from_(input_tiled.subquery())
            # TODO: composite join keys
            .group_by("tile_index", self.key)
            .order_by("tile_index")
        )

        return groupby_sql


def make_literal_value(value: Any) -> expressions.Literal:
    """Create a sqlglot literal value

    Parameters
    ----------
    value : Any
        The literal value

    Returns
    -------
    expressions.Literal
    """
    if isinstance(value, str):
        return expressions.Literal.string(value)
    return expressions.Literal.number(value)


def make_binary_operation_node(
    node_type: NodeType,
    input_sql_nodes: List[SQLNode],
    parameters: Dict[str, Any],
) -> BinaryOp:
    """Create a BinaryOp node for eligible query node types

    Parameters
    ----------
    node_type : NodeType
        Node type
    input_sql_nodes : List[SQLNode]
        List of input SQL nodes
    parameters : dict
        Query node parameters

    Returns
    -------
    BinaryOp

    Raises
    ------
    NotImplementedError
        For incompatible node types
    """
    node_type_to_expression_cls = {
        # Arithmetic
        NodeType.ADD: expressions.Add,
        NodeType.SUB: expressions.Sub,
        NodeType.MUL: expressions.Mul,
        NodeType.DIV: expressions.Div,
        # Relational
        NodeType.EQ: expressions.EQ,
        NodeType.NE: expressions.NEQ,
        NodeType.LT: expressions.LT,
        NodeType.LE: expressions.LTE,
        NodeType.GT: expressions.GT,
        NodeType.GE: expressions.GTE,
        # Logical
        NodeType.AND: expressions.And,
        NodeType.OR: expressions.Or,
    }
    expression_cls = node_type_to_expression_cls.get(node_type)

    if expression_cls is None:
        raise NotImplementedError(f"{node_type} cannot be converted to binary operation")

    left = input_sql_nodes[0]
    right: Any
    if len(input_sql_nodes) == 1:
        # Series <> scalar
        literal_value = make_literal_value(parameters["value"])
        right = ExpressionNode(literal_value)
    else:
        # Series <> Series
        right = input_sql_nodes[1]

    output_node = BinaryOp(left=left, right=right, operation=expression_cls)
    return output_node
