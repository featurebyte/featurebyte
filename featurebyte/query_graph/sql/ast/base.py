"""
Module containing base classes and functions for building syntax tree
"""
from __future__ import annotations

from typing import Any, Optional, Type, TypeVar

from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field

from sqlglot import Expression, expressions, parse_one, select

from featurebyte.common.typing import is_scalar_nan
from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.common import SQLType

SQLNodeT = TypeVar("SQLNodeT", bound="SQLNode")
TableNodeT = TypeVar("TableNodeT", bound="TableNode")


@dataclass
class SQLNodeContext:
    """
    Context containing information required when constructing instances of SQLNode

    Parameters
    ----------
    query_node : Node
        Query graph node
    sql_type: SQLType
        Type of SQL code to generate
    source_type : SourceType
        Type of the data warehouse that the SQL will run on
    groupby_keys : list[str] | None
        List of groupby keys that is used for the downstream groupby operation. This information is
        required so that only tiles corresponding to specific entities are built (vs building tiles
        using all available data). This option is only used when SQLType is BUILD_TILE_ON_DEMAND.
    input_sql_nodes : list[SQLNode]
        List of input SQL nodes
    """

    query_node: Node
    sql_type: SQLType
    source_type: SourceType
    groupby_keys: list[str] | None
    input_sql_nodes: list[SQLNode]

    def __post_init__(self) -> None:
        self.parameters = self.query_node.parameters.dict()


@dataclass  # type: ignore
class SQLNode(ABC):
    """Base class of a node in the SQL operations tree

    Query Graph Interpreter constructs a tree that represents the list of SQL operations required to
    produce the feature described by the Query Graph. Each SQL operation can be represented as a
    node in this tree. This is the interface that a node in this tree should implement.

    query_node_type attribute specifies the type of the query graph node that the SQLNode
    corresponds to. If query_node_type is not overridden, the class will not be picked up by the
    NodeRegistry and has to be manually instantiated.
    """

    context: SQLNodeContext
    query_node_type: Optional[NodeType | list[NodeType]] = field(init=False, default=None)

    @property
    @abstractmethod
    def sql(self) -> Expression | expressions.Subqueryable:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """

    @classmethod
    def build(  # pylint: disable=useless-return
        cls: Type[SQLNodeT], context: SQLNodeContext
    ) -> Optional[SQLNodeT]:
        """Create an instance of SQLNode given a context if applicable

        Parameters
        ----------
        context : SQLNodeContext
            Context for building SQLNode

        Returns
        -------
        Optional[SQLNodeT]
        """
        _ = context
        return None


@dataclass  # type: ignore[misc]
class TableNode(SQLNode, ABC):
    """Nodes that produce table-like output that can be used as nested input

    Parameters
    ----------
    columns_map : dict[str, Expression]
        This mapping keeps track of the expression currently associated with each column name
    columns_node : dict[str, ExpressionNode]
        Mapping from column name to ExpressionNode for assigned columns
    where_condition : Optional[Expression]
        Expression to be used in WHERE clause
    qualify_condition : Optional[Expression]
        Expression to be used in QUALIFY clause
    """

    columns_map: dict[str, Expression]
    columns_node: dict[str, ExpressionNode] = field(init=False)
    where_condition: Optional[Expression] = field(init=False)
    qualify_condition: Optional[Expression] = field(init=False)

    def __post_init__(self) -> None:
        self.columns_node = {}
        self.where_condition = None
        self.qualify_condition = None

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

    def assign_column(self, column_name: str, node: ExpressionNode) -> None:
        """Performs an assignment and update column_name's expression

        Parameters
        ----------
        column_name : str
            Column name
        node : ExpressionNode
            An instance of ExpressionNode
        """
        self.columns_map[column_name] = node.sql
        self.columns_node[column_name] = node

    def get_column_node(self, column_name: str) -> ExpressionNode | None:
        """Get SQLNode for a column

        Parameters
        ----------
        column_name : str
            Column name

        Returns
        -------
        SQLNode | None
        """
        return self.columns_node.get(column_name)

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

    def subset_columns(self: TableNodeT, columns: list[str]) -> TableNodeT:
        """Create a new TableNode with subset of columns

        Parameters
        ----------
        columns : list[str]
            Selected column names

        Returns
        -------
        TableNode
        """
        columns_set = set(columns)
        subset_columns_map = {
            column_name: expr
            for (column_name, expr) in self.columns_map.items()
            if column_name in columns_set
        }
        subset_columns_node = {
            column_name: node
            for (column_name, node) in self.columns_node.items()
            if column_name in columns_set
        }
        subset_table = self.copy()
        subset_table.columns_map = subset_columns_map
        subset_table.columns_node = subset_columns_node
        return subset_table

    def subset_rows(self: TableNodeT, condition: Expression) -> TableNodeT:
        """Return a new InputNode with rows filtered

        Parameters
        ----------
        condition : Expression
            Condition expression to be used for filtering

        Returns
        -------
        TableNodeT
        """
        out = self.copy()
        if has_window_function(condition):
            assert self.qualify_condition is None
            out.qualify_condition = condition
        else:
            if self.where_condition is not None:
                out.where_condition = expressions.and_(self.where_condition, condition)
            else:
                out.where_condition = condition
        return out

    def copy(self: TableNodeT) -> TableNodeT:
        """Create a copy of this TableNode

        Returns
        -------
        TableNode
        """
        return deepcopy(self)


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


def make_literal_value(value: Any, cast_as_timestamp: bool = False) -> expressions.Expression:
    """Create a sqlglot literal value

    Parameters
    ----------
    value : Any
        The literal value
    cast_as_timestamp : bool
        Whether to cast the value to timestamp

    Returns
    -------
    Expression
    """
    if is_scalar_nan(value):
        return expressions.Null()
    if cast_as_timestamp:
        return parse_one(f"CAST('{str(value)}' AS TIMESTAMP)")
    if isinstance(value, str):
        return expressions.Literal.string(value)
    return expressions.Literal.number(value)


def has_window_function(expression: Expression) -> bool:
    """
    Returns whether the expression contains a window function

    Parameters
    ----------
    expression : Expression
        Expression to check

    Returns
    -------
    bool
    """
    return len(list(expression.find_all(expressions.Window))) > 0
