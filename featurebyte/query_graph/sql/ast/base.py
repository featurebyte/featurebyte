"""
Module containing base classes and functions for building syntax tree
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from copy import copy
from dataclasses import dataclass, field
from typing import Optional, Type, TypeVar, cast

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, select

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.adapter import BaseAdapter, get_sql_adapter
from featurebyte.query_graph.sql.common import (
    EventTableTimestampFilter,
    OnDemandEntityFilters,
    PartitionColumnFilters,
    SQLType,
)
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.specs import AggregationSpec

SQLNodeT = TypeVar("SQLNodeT", bound="SQLNode")
TableNodeT = TypeVar("TableNodeT", bound="TableNode")


@dataclass
class SQLNodeContext:
    """
    Context containing information required when constructing instances of SQLNode

    graph : QueryGraphModel
        Query graph
    query_node : Node
        Query graph node
    sql_type: SQLType
        Type of SQL code to generate
    source_type : SourceType
        Type of the data warehouse that the SQL will run on
    input_sql_nodes : list[SQLNode]
        List of input SQL nodes
    to_filter_scd_by_current_flag: Optional[bool]
        Whether to filter SCD table with current flag
    """

    graph: QueryGraphModel
    query_node: Node
    sql_type: SQLType
    source_info: SourceInfo
    input_sql_nodes: list[SQLNode]
    to_filter_scd_by_current_flag: Optional[bool]
    event_table_timestamp_filter: Optional[EventTableTimestampFilter]
    aggregation_specs: Optional[dict[str, list[AggregationSpec]]]
    on_demand_entity_filters: Optional[OnDemandEntityFilters]
    partition_column_filters: Optional[PartitionColumnFilters]

    def __post_init__(self) -> None:
        self.parameters = self.query_node.parameters.model_dump()
        self.current_query_node = self.query_node
        if self.to_filter_scd_by_current_flag is None:
            self.to_filter_scd_by_current_flag = False

    @property
    def adapter(self) -> BaseAdapter:
        """
        Adapter object for generating engine specific SQL expressions

        Returns
        -------
        BaseAdapter
        """
        return get_sql_adapter(self.source_info)


@dataclass
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
    def sql(self) -> Expression | expressions.Query:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """

    @classmethod
    def build(cls: Type[SQLNodeT], context: SQLNodeContext) -> Optional[SQLNodeT]:
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


@dataclass
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

    @property
    def sql(self) -> Expression:
        exprs = list(self.columns_map.values())
        aliases = list(self.columns_map.keys())
        return self.get_sql_for_expressions(exprs=exprs, aliases=aliases)

    def get_select_statement_without_columns(self) -> Select:
        """
        Construct a Select statement for this table but without the columns

        Returns
        -------
        Select
        """

        # QUALIFY clause if supported
        if (
            self.qualify_condition is not None
            and self.context.adapter.is_qualify_clause_supported()
        ):
            qualify_expr = expressions.Qualify(this=self.qualify_condition)
            select_expr = expressions.Select(qualify=qualify_expr)
        else:
            select_expr = select()

        # FROM clause
        select_expr = self.from_query_impl(select_expr)

        # WHERE clause
        if self.where_condition is not None:
            select_expr = select_expr.where(self.where_condition)

        return select_expr

    def from_query_impl(self, select_expr: Select) -> Select:
        """Construct the FROM clause in the Select expression

        The provided select_expr is a partially constructed Select expression formed using
        information from attributes such as columns_map and where conditions. In most cases,
        subclasses will construct the FROM clause using select_expr as the starting point.

        The default implementation is no-op and most subclasses should override it.

        Parameters
        ----------
        select_expr: Select
            Partially constructed Select expression

        Returns
        -------
        Select
        """
        return select_expr

    def sql_nested(self) -> Expression:
        """SQL expression that can be used within from_() to form a nested query

        Returns
        -------
        Expression
            Expression that can be used within from_()
        """
        sql = cast(expressions.Query, self.sql)
        return cast(expressions.Expression, sql.subquery())

    @property
    def require_nested_filter_post_select(self) -> bool:
        """
        Whether there is a need to apply a nested filter after the Select statement

        Returns
        -------
        bool
        """
        return (
            self.qualify_condition is not None
            and not self.context.adapter.is_qualify_clause_supported()
        )

    def get_sql_for_expressions(
        self,
        exprs: list[Expression],
        aliases: Optional[list[str]] = None,
    ) -> Expression:
        """
        Construct a Select statement using expr within the context of this table

        Parameters
        ----------
        exprs: list[Expression]
            Expressions
        aliases: Optional[list[str]]
            Aliases of the expressions

        Returns
        -------
        Select
        """
        if aliases is not None:
            assert len(exprs) == len(aliases)

        if aliases is None and self.require_nested_filter_post_select:
            # When previewing an unnamed expression, e.g. ("a" + "b") but at the same time a nested
            # filter is required, we need to give ("a" + "b") a name so that we can remove the
            # temporary nested filter column in the output.
            aliases = [f"Unnamed{i}" for i in range(len(exprs))]

        if aliases is None:
            named_exprs = exprs
        else:
            named_exprs = [
                expressions.alias_(expr, alias=alias, quoted=True)
                for (expr, alias) in zip(exprs, aliases)
            ]

        select_expr = self.get_select_statement_without_columns()

        # Specify required expressions / columns for the SELECT clause
        select_expr = select_expr.select(*named_exprs)

        # Use nested filter if QUALIFY clause is not supported
        if self.require_nested_filter_post_select:
            assert aliases is not None
            assert self.qualify_condition is not None
            select_expr = self.context.adapter.filter_with_window_function(
                select_expr, aliases, self.qualify_condition
            )

        return select_expr

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

    def subset_columns(self: TableNodeT, context: SQLNodeContext, columns: list[str]) -> TableNodeT:
        """Create a new TableNode with subset of columns

        Parameters
        ----------
        context: SQLNodeContext
            Metadata such as the query graph and node associated with the SQLNode to be produced
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
        subset_table.context.current_query_node = context.query_node
        subset_table.columns_map = subset_columns_map
        subset_table.columns_node = subset_columns_node
        return subset_table

    def subset_rows(self: TableNodeT, context: SQLNodeContext, condition: Expression) -> TableNodeT:
        """Return a new InputNode with rows filtered

        Parameters
        ----------
        context: SQLNodeContext
            Metadata such as the query graph and node associated with the SQLNode to be produced
        condition : Expression
            Condition expression to be used for filtering

        Returns
        -------
        TableNodeT
        """
        out = self.copy()
        out.context.current_query_node = context.query_node
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
        new_table = copy(self)
        new_table.context = copy(self.context)
        new_table.columns_map = copy(self.columns_map)
        new_table.columns_node = copy(self.columns_node)
        return new_table


@dataclass
class ExpressionNode(SQLNode, ABC):
    """Base class for all expression nodes (non-table)"""

    table_node: Optional[TableNode]

    @property
    def sql_standalone(self) -> Expression:
        """Construct a sql expression that produces a table output for preview purpose

        Returns
        -------
        Expression
            A sqlglot Expression object
        """
        assert self.table_node is not None
        return self.table_node.get_sql_for_expressions([self.sql])


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
