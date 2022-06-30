"""
This module contains the list of SQL operations to be used by the Query Graph Interpreter
"""
from __future__ import annotations

from typing import Any

# pylint: disable=too-few-public-methods
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum

from sqlglot import Expression, expressions, parse_one, select

from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.tiling import TileSpec, get_aggregator


class SQLType(Enum):
    """Type of SQL code corresponding to different operations"""

    BUILD_TILE = "build_tile"
    PREVIEW = "preview"


def escape_column_name(column_name: str) -> str:
    """Enclose provided column name with quotes

    Parameters
    ----------
    column_name : str
        Column name

    Returns
    -------
    str
    """
    if column_name.startswith('"') and column_name.endswith('"'):
        return column_name
    return f'"{column_name}"'


def escape_column_names(column_names: list[str]) -> list[str]:
    """Enclose provided column names with quotes

    Parameters
    ----------
    column_names : list[str]
        Column names

    Returns
    -------
    list[str]
    """
    return [escape_column_name(x) for x in column_names]


class SQLNode(ABC):
    """Base class of a node in the SQL operations tree

    Query Graph Interpreter constructs a tree that represents the list of SQL operations required to
    produce the feature described by the Query Graph. Each SQL operation can be represented as a
    node in this tree. This is the interface that a node in this tree should implement.
    """

    @property
    @abstractmethod
    def sql(self) -> Expression | expressions.Subqueryable:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """


@dataclass  # type: ignore[misc]
class TableNode(SQLNode, ABC):
    """Nodes that produce table-like output that can be used as nested input

    Parameters
    ----------
    columns_map : dict[str, Expression]
        This mapping keeps track of the expression currently associated with each column name
    """

    columns_map: dict[str, Expression]

    @property
    def columns(self) -> list[str]:
        """Columns that are available in this table

        Returns
        -------
        List[str]
            List of column names
        """
        return list(self.columns_map.keys())

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

    def set_column_expr(self, column_name: str, expr: Expression) -> None:
        """Set expression for a column name

        Parameters
        ----------
        column_name : str
            Column name
        expr : Expression
            SQL expression
        """
        self.columns_map[column_name] = expr

    def get_column_expr(self, column_name: str) -> Expression:
        """Get expression for a column name

        Parameters
        ----------
        column_name : str
            Column name

        Returns
        -------
        Expression
        """
        return self.columns_map[column_name]

    def set_columns_map(self, columns_map: dict[str, Expression]) -> None:
        """Set column-expression mapping to the provided mapping

        The default implementation simply sets self.columns_map to the provided dict. However, nodes
        such as FilterFrame need to override this.

        Parameters
        ----------
        columns_map : dict[str, Expression]
            Column names to expressions mapping
        """
        self.columns_map = columns_map


@dataclass  # type: ignore
class ExpressionNode(SQLNode, ABC):
    """Base class for all expression nodes (non-table)"""

    table_node: TableNode

    @property
    def sql_standalone(self) -> Expression:
        """Construct a sql expression that produces a table output for preview purpose

        Returns
        -------
        Expression
            A sqlglot Expression object
        """
        return select(self.sql).from_(self.table_node.sql_nested())


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
class GenericInputNode(TableNode):
    """Input data node"""

    column_names: list[str]
    dbtable: str
    database_source: dict[str, Any]

    @property
    def sql(self) -> Expression:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """
        select_args = []
        for col, expr in self.columns_map.items():
            col = expressions.Identifier(this=col, quoted=True)
            select_args.append(expressions.alias_(expr, col))
        select_expr = select(*select_args)
        if self.database_source["type"] == SourceType.SNOWFLAKE:
            details = self.database_source["details"]
            database = details["database"]
            sf_schema = details["sf_schema"]
            dbtable = f'"{database}"."{sf_schema}"."{self.dbtable}"'
        else:
            dbtable = escape_column_name(self.dbtable)
        select_expr = select_expr.from_(dbtable)
        return select_expr


@dataclass
class BuildTileInputNode(GenericInputNode):
    """Input data node used when building tiles"""

    timestamp: str

    @property
    def sql(self) -> Expression:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """
        select_expr = super().sql
        assert isinstance(select_expr, expressions.Select)
        timestamp = escape_column_name(self.timestamp)
        select_expr = select_expr.where(
            f"{timestamp} >= CAST(FBT_START_DATE AS TIMESTAMP)",
            f"{timestamp} < CAST(FBT_END_DATE AS TIMESTAMP)",
        )
        return select_expr


@dataclass
class BinaryOp(ExpressionNode):
    """Binary operation node"""

    left_node: ExpressionNode
    right_node: ExpressionNode
    operation: type[expressions.Expression]

    @property
    def sql(self) -> Expression:
        return self.operation(this=self.left_node.sql, expression=self.right_node.sql)


@dataclass
class Project(ExpressionNode):
    """Project node for a single column"""

    column_name: str

    @property
    def sql(self) -> Expression:
        return self.table_node.get_column_expr(self.column_name)


@dataclass
class FilteredFrame(TableNode):
    """Filter node for table"""

    input_node: TableNode
    mask: ExpressionNode

    def set_columns_map(self, columns_map: dict[str, Expression]) -> None:
        """Set column-expression mapping to the provided mapping

        This overrides the default implementation because FilteredFrame offloads generation of the
        pre-filtered SQL to the input_node. Setting columns_map attribute of FilteredFrame itself
        only has effect if the columns_map is also applied to the input_node as well.

        One scenario that is affected by this is Filter then Project.

        Parameters
        ----------
        columns_map : dict[str, Expression]
            Column names to expressions mapping
        """
        self.input_node.set_columns_map(columns_map)
        super().set_columns_map(columns_map)

    @property
    def sql(self) -> Expression:
        input_sql = self.input_node.sql
        assert isinstance(input_sql, expressions.Select)
        return input_sql.where(self.mask.sql)


@dataclass
class FilteredSeries(ExpressionNode):
    """Filter node for series"""

    series_node: ExpressionNode
    mask: ExpressionNode

    @property
    def sql(self) -> Expression:
        return self.series_node.sql

    @property
    def sql_standalone(self) -> Expression:
        pre_filter_sql = super().sql_standalone
        assert isinstance(pre_filter_sql, expressions.Select)
        return pre_filter_sql.where(self.mask.sql)


@dataclass
class BuildTileNode(TableNode):
    """Tile builder node

    This node is responsible for generating the tile building SQL for a groupby operation.
    """

    input_node: TableNode
    keys: list[str]
    tile_specs: list[TileSpec]
    timestamp: str
    agg_func: str
    frequency: int

    @property
    def sql(self) -> Expression:
        start_date_placeholder = "FBT_START_DATE"
        start_date_placeholder_epoch = (
            f"DATE_PART(EPOCH_SECOND, CAST({start_date_placeholder} AS TIMESTAMP))"
        )
        timestamp_epoch = f"DATE_PART(EPOCH_SECOND, {escape_column_name(self.timestamp)})"

        input_tiled = select(
            "*",
            f"FLOOR(({timestamp_epoch} - {start_date_placeholder_epoch}) / {self.frequency}) AS tile_index",
        ).from_(self.input_node.sql_nested())

        tile_start_date = (
            f"TO_TIMESTAMP({start_date_placeholder_epoch} + tile_index * {self.frequency})"
        )
        keys = escape_column_names(self.keys)
        groupby_sql = (
            select(
                f"{tile_start_date} AS tile_start_date",
                *keys,
                *[f"{spec.tile_expr} AS {spec.tile_column_name}" for spec in self.tile_specs],
            )
            .from_(input_tiled.subquery())
            .group_by("tile_index", *keys)
            .order_by("tile_index")
        )

        return groupby_sql


BINARY_OPERATION_NODE_TYPES = {
    NodeType.ADD,
    NodeType.SUB,
    NodeType.MUL,
    NodeType.DIV,
    NodeType.EQ,
    NodeType.NE,
    NodeType.LT,
    NodeType.LE,
    NodeType.GT,
    NodeType.GE,
    NodeType.AND,
    NodeType.OR,
}


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
    input_sql_nodes: list[SQLNode],
    parameters: dict[str, Any],
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
    assert sorted(node_type_to_expression_cls.keys()) == sorted(BINARY_OPERATION_NODE_TYPES)
    expression_cls = node_type_to_expression_cls.get(node_type)

    if expression_cls is None:
        raise NotImplementedError(f"{node_type} cannot be converted to binary operation")

    left_node = input_sql_nodes[0]
    assert isinstance(left_node, ExpressionNode)
    table_node = left_node.table_node
    right_node: Any
    if len(input_sql_nodes) == 1:
        # When the other value is a scalar
        literal_value = make_literal_value(parameters["value"])
        right_node = ParsedExpressionNode(table_node=table_node, expr=literal_value)
    else:
        # When the other value is a Series
        right_node = input_sql_nodes[1]

    if isinstance(right_node, ExpressionNode) and parameters.get("right_op"):
        # Swap left & right objects if the operation from the right object
        left_node, right_node = right_node, left_node

    output_node = BinaryOp(
        table_node=table_node,
        left_node=left_node,
        right_node=right_node,
        operation=expression_cls,
    )
    return output_node


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
        columns_set = set(columns)
        columns_map = {
            column_name: expr
            for (column_name, expr) in table_node.columns_map.items()
            if column_name in columns_set
        }
        subset_table = deepcopy(table_node)
        subset_table.set_columns_map(columns_map)
        sql_node = subset_table
    return sql_node


def make_filter_node(
    input_sql_nodes: list[SQLNode], output_type: NodeOutputType
) -> FilteredFrame | FilteredSeries:
    """Create a FilteredFrame or FilteredSeries node

    Parameters
    ----------
    input_sql_nodes : list[SQLNode]
        List of input SQL nodes
    output_type : NodeOutputType
        Query node output type

    Returns
    -------
    FilteredFrame | FilteredSeries
        The appropriate SQL node for projection
    """
    item, mask = input_sql_nodes
    assert isinstance(mask, ExpressionNode)
    sql_node: FilteredFrame | FilteredSeries
    if output_type == NodeOutputType.FRAME:
        assert isinstance(item, TableNode)
        input_table_copy = deepcopy(item)
        sql_node = FilteredFrame(
            columns_map=input_table_copy.columns_map,
            input_node=input_table_copy,
            mask=mask,
        )
    else:
        assert isinstance(item, ExpressionNode)
        sql_node = FilteredSeries(table_node=item.table_node, series_node=item, mask=mask)
    return sql_node


def make_build_tile_node(
    input_sql_nodes: list[SQLNode], parameters: dict[str, Any]
) -> BuildTileNode:
    """Create a BuildTileNode

    Parameters
    ----------
    input_sql_nodes : list[SQLNode]
        List of input SQL nodes
    parameters : dict[str, Any]
        Query node parameters

    Returns
    -------
    BuildTileNode
    """
    input_node = input_sql_nodes[0]
    assert isinstance(input_node, TableNode)
    aggregator = get_aggregator(parameters["agg_func"])
    tile_specs = aggregator.tile(parameters["parent"])
    columns = (
        ["tile_start_date"] + parameters["keys"] + [spec.tile_column_name for spec in tile_specs]
    )
    columns_map = {col: expressions.Identifier(this=col, quoted=True) for col in columns}
    sql_node = BuildTileNode(
        columns_map=columns_map,
        input_node=input_node,
        keys=parameters["keys"],
        tile_specs=tile_specs,
        timestamp=parameters["timestamp"],
        agg_func=parameters["agg_func"],
        frequency=parameters["frequency"],
    )
    return sql_node


def make_input_node(
    parameters: dict[str, Any],
    sql_type: SQLType,
) -> BuildTileInputNode | GenericInputNode:
    """Create a SQLNode corresponding to a query graph input node

    Parameters
    ----------
    parameters : dict[str, Any]
        Query graph node parameters
    sql_type: SQLType
        Type of SQL code to generate

    Returns
    -------
    BuildTileInputNode | GenericInputNode
        SQLNode corresponding to the query graph input node
    """
    columns_map = {}
    for colname in parameters["columns"]:
        columns_map[colname] = expressions.Identifier(this=colname, quoted=True)
    sql_node: BuildTileInputNode | GenericInputNode
    database_source = parameters["database_source"]
    if sql_type == SQLType.BUILD_TILE:
        sql_node = BuildTileInputNode(
            columns_map=columns_map,
            column_names=parameters["columns"],
            timestamp=parameters["timestamp"],
            dbtable=parameters["dbtable"],
            database_source=database_source,
        )
    else:
        sql_node = GenericInputNode(
            columns_map=columns_map,
            column_names=parameters["columns"],
            dbtable=parameters["dbtable"],
            database_source=database_source,
        )
    return sql_node
