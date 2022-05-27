"""
This module contains the list of SQL operations to be used by the Query Graph Interpreter
"""
from typing import Any, List

# pylint: disable=W0511
# pylint: disable=R0903
from abc import ABC, abstractmethod
from dataclasses import dataclass

from sqlglot import Expression, expressions, parse_one, select


class SQLNode(ABC):
    """Base class of a node in the SQL operations tree

    Query Graph Interpreter constructs a tree that represents the list of SQL operations required to
    produce the feature described by the Query Graph. Each SQL operation can be represented as a
    node in this tree. This is the interface that a node in this tree should implement.
    """

    @property
    @abstractmethod
    def sql(self) -> Expression:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """


@dataclass
class InputNode(SQLNode):
    """Input data node used when building tiles"""

    columns: List[str]
    timestamp: str
    input: SQLNode

    @property
    def sql(self) -> Expression:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """
        select_expr = select(*self.columns)
        if isinstance(self.input, ExpressionNode):
            select_expr = select_expr.from_(self.input.sql)
        else:
            select_expr = select_expr.from_(self.input.sql.subquery())
        # TODO: this is only for tile-gen sql
        select_expr = select_expr.where(
            f"{self.timestamp} >= CAST(FBT_START_DATE AS TIMESTAMP)",
            f"{self.timestamp} < CAST(FBT_END_DATE AS TIMESTAMP)",
        )
        return select_expr


@dataclass
class ExpressionNode(SQLNode):
    """Expression node"""

    expr: Expression

    @property
    def sql(self) -> Expression:
        return self.expr


@dataclass
class AddNode(SQLNode):
    """Add node"""

    left: SQLNode
    right: SQLNode

    @property
    def sql(self) -> Expression:
        return parse_one(f"{self.left.sql.sql()} + {self.right.sql.sql()}")


@dataclass
class Project(SQLNode):
    """Project node"""

    columns: List[str]

    @property
    def sql(self) -> Expression:
        assert len(self.columns) == 1
        return parse_one(self.columns[0])


@dataclass
class AssignNode(SQLNode):
    """Assign node"""

    table: Any
    column: SQLNode
    name: str

    def __post_init__(self) -> None:
        if self.table.columns is None:
            raise RuntimeError(f"{self.table} has no columns attribute")
        self.columns = [x for x in self.table.columns if x != self.name] + [self.name]

    @property
    def sql(self) -> Expression:
        existing_columns = [col for col in self.table.columns if col != self.name]
        select_expr = select(*existing_columns)
        select_expr = select_expr.select(expressions.alias_(self.column.sql, self.name))
        select_expr = select_expr.from_(self.table.sql.subquery())
        return select_expr


@dataclass
class BuildTileNode(SQLNode):
    """Tile builder node

    This node is responsible for generating the tile building SQL for a groupby operation.
    """

    input: SQLNode
    key: str
    parent: str
    timestamp: str
    agg_func: str
    frequency: int

    def __post_init__(self) -> None:
        self.columns = ["tile_start_date", self.key, "value"]

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
        ).from_(self.input.sql.subquery())

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
