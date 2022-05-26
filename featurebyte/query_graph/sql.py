from typing import List

from dataclasses import dataclass

import sqlglot
from sqlglot import expressions, parse_one, select

__all__ = [
    "InputNode",
    "ExpressionNode",
    "AddNode",
    "Project",
    "AssignNode",
    "BuildTileNode",
]


class SQLNode:
    @property
    def sql(self):
        raise NotImplementedError()


@dataclass
class InputNode(SQLNode):
    columns: List[str]
    timestamp: str
    input: SQLNode

    @property
    def sql(self):
        s = select()
        for c in self.columns:
            s = s.select(c)
        if isinstance(self.input, ExpressionNode):
            s = s.from_(self.input.sql)
        else:
            s = s.from_(self.input.sql.subquery())
        # TODO: this is only for tile-gen sql
        s = s.where(
            f"{self.timestamp} >= CAST(FBT_START_DATE AS TIMESTAMP)",
            f"{self.timestamp} < CAST(FBT_END_DATE AS TIMESTAMP)",
        )
        return s


@dataclass
class ExpressionNode(SQLNode):
    expr: sqlglot.Expression

    @property
    def sql(self):
        return self.expr


@dataclass
class AddNode(SQLNode):
    left: ExpressionNode
    right: ExpressionNode

    @property
    def sql(self):
        return parse_one(f"{self.left.sql.sql()} + {self.right.sql.sql()}")


@dataclass
class Project(SQLNode):
    columns: list[str]

    @property
    def sql(self):
        assert len(self.columns) == 1
        return parse_one(self.columns[0])


@dataclass
class AssignNode(SQLNode):
    table: InputNode  # TODO: can also be AssignNode. FilterNode?
    column: ExpressionNode
    name: str

    def __post_init__(self):
        self.columns = [x for x in self.table.columns if x not in self.name] + [self.name]

    @property
    def sql(self):
        s = select()
        for col in self.table.columns:
            if col == self.name:
                continue
            s = s.select(col)
        s = s.select(expressions.alias_(self.column.sql, self.name))
        s = s.from_(self.table.sql.subquery())
        return s


@dataclass
class BuildTileNode:
    input: SQLNode
    key: str
    parent: str
    timestamp: str
    agg_func: str
    frequency: int

    def __post_init__(self):
        self.columns = ["tile_start_date", self.key, "value"]

    def tile_sql(self):
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
