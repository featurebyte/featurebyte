"""
This module contains the list of SQL operations to be used by the Query Graph Interpreter
"""
from __future__ import annotations

from typing import Any, Literal, Optional, TypeVar, Union

# pylint: disable=too-few-public-methods,too-many-lines
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum

import pandas as pd
from sqlglot import Expression, expressions, parse_one, select

from featurebyte.common.typing import (
    DatetimeSupportedPropertyType,
    TimedeltaSupportedUnitType,
    is_scalar_nan,
)
from featurebyte.enum import DBVarType, InternalName, SourceType
from featurebyte.query_graph import expression as fb_expressions
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.feature_common import AggregationSpec
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.tiling import TileSpec, get_aggregator

MISSING_VALUE_REPLACEMENT = "__MISSING__"

TableNodeT = TypeVar("TableNodeT", bound="TableNode")


class SQLType(Enum):
    """Type of SQL code corresponding to different operations"""

    BUILD_TILE = "build_tile"
    BUILD_TILE_ON_DEMAND = "build_tile_on_demand"
    EVENT_VIEW_PREVIEW = "event_view_preview"
    GENERATE_FEATURE = "generate_feature"


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

    def set_columns_map(self, columns_map: dict[str, Expression]) -> None:
        """Set column-expression mapping to the provided mapping

        Parameters
        ----------
        columns_map : dict[str, Expression]
            Column names to expressions mapping
        """
        self.columns_map = columns_map

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
class InputNode(TableNode):
    """Input data node"""

    dbtable: dict[str, str]
    feature_store: dict[str, Any]

    @property
    def sql(self) -> Expression:
        """Construct a sql expression

        Returns
        -------
        Expression
            A sqlglot Expression object
        """
        # QUALIFY clause
        if self.qualify_condition is not None:
            qualify_expr = expressions.Qualify(this=self.qualify_condition)
            select_expr = expressions.Select(qualify=qualify_expr)
        else:
            select_expr = select()

        # SELECT clause
        select_args = []
        for col, expr in self.columns_map.items():
            col = expressions.Identifier(this=col, quoted=True)
            select_args.append(expressions.alias_(expr, col))
        select_expr = select_expr.select(*select_args)

        # FROM clause
        if self.feature_store["type"] == SourceType.SNOWFLAKE:
            database = self.dbtable["database_name"]
            schema = self.dbtable["schema_name"]
            table = self.dbtable["table_name"]
            dbtable = f'"{database}"."{schema}"."{table}"'
        else:
            dbtable = escape_column_name(self.dbtable["table_name"])
        select_expr = select_expr.from_(dbtable)

        # WHERE clause
        if self.where_condition is not None:
            select_expr = select_expr.where(self.where_condition)

        return select_expr


@dataclass
class BuildTileInputNode(InputNode):
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
        table_expr = super().sql
        assert isinstance(table_expr, expressions.Select)
        # Apply tile start and end date filters on a nested subquery to avoid filtering out data
        # required by window function
        select_expr = select("*").from_(table_expr.subquery())
        timestamp = escape_column_name(self.timestamp)
        start_cond = (
            f"{timestamp} >= CAST({InternalName.TILE_START_DATE_SQL_PLACEHOLDER} AS TIMESTAMP)"
        )
        end_cond = f"{timestamp} < CAST({InternalName.TILE_END_DATE_SQL_PLACEHOLDER} AS TIMESTAMP)"
        select_expr = select_expr.where(start_cond, end_cond)
        return select_expr


@dataclass
class SelectedEntityBuildTileInputNode(InputNode):
    """Input data node used when building tiles for selected entities only

    The selected entities are expected to be available in an "entity table". It can be injected as a
    subquery by replacing the placeholder InternalName.ENTITY_TABLE_SQL_PLACEHOLDER.

    Entity table is expected to have these columns:
    * entity column(s)
    * InternalName.ENTITY_TABLE_START_DATE
    * InternalName.ENTITY_TABLE_END_DATE

    Entity column(s) is expected to be unique in the entity table (in the primary key sense).
    """

    timestamp: str
    entity_columns: list[str]

    @property
    def sql(self) -> Expression:

        entity_table = InternalName.ENTITY_TABLE_NAME.value
        start_date = InternalName.ENTITY_TABLE_START_DATE.value
        end_date = InternalName.ENTITY_TABLE_END_DATE.value

        join_conditions = []
        for col in self.entity_columns:
            condition = parse_one(f'R."{col}" = {entity_table}."{col}"')
            join_conditions.append(condition)
        join_conditions.append(parse_one(f'R."{self.timestamp}" >= {entity_table}.{start_date}'))
        join_conditions.append(parse_one(f'R."{self.timestamp}" < {entity_table}.{end_date}'))
        join_conditions_expr = expressions.and_(*join_conditions)

        table_sql = super().sql
        result = (
            select()
            .with_(entity_table, as_=InternalName.ENTITY_TABLE_SQL_PLACEHOLDER.value)
            .select("R.*", start_date)
            .from_(entity_table)
            .join(
                table_sql,
                join_alias="R",
                join_type="left",
                on=join_conditions_expr,
            )
        )
        return result


@dataclass
class UnaryOp(ExpressionNode):
    """Typical unary operation node"""

    expr: ExpressionNode
    operation: type[expressions.Expression]

    @property
    def sql(self) -> Expression:
        return self.operation(this=self.expr.sql)


@dataclass
class BinaryOp(ExpressionNode):
    """Binary operation node"""

    left_node: ExpressionNode
    right_node: ExpressionNode
    operation: type[expressions.Expression]

    @property
    def sql(self) -> Expression:
        right_expr = self.right_node.sql
        if self.operation in {expressions.Div, expressions.Mod}:
            # Make 0 divisor null to prevent division-by-zero error
            right_expr = parse_one(f"NULLIF({right_expr.sql()}, 0)")
        if self.operation == fb_expressions.Concat:
            op_expr = self.operation(expressions=[self.left_node.sql, right_expr])
        elif self.operation == expressions.Pow:
            op_expr = self.operation(this=self.left_node.sql, power=right_expr)
        else:
            op_expr = self.operation(this=self.left_node.sql, expression=right_expr)
        return expressions.Paren(this=op_expr)


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
        return select(escape_column_name(self.column_name)).from_(self.table_node.sql_nested())


@dataclass
class AliasNode(ExpressionNode):
    """Alias node that represents assignment to FeatureGroup"""

    name: str
    expr_node: ExpressionNode

    @property
    def sql(self) -> Expression:
        return self.expr_node.sql


@dataclass
class Conditional(ExpressionNode):
    """Conditional node"""

    series_node: ExpressionNode
    mask: ExpressionNode
    value: Any

    @property
    def sql(self) -> Expression:
        if_expr = expressions.If(this=self.mask.sql, true=make_literal_value(self.value))
        expr = expressions.Case(ifs=[if_expr], default=self.series_node.sql)
        return expr


@dataclass
class IsNullNode(ExpressionNode):
    """Node for IS_NULL operation"""

    expr: ExpressionNode

    @property
    def sql(self) -> Expression:
        return expressions.Is(this=self.expr.sql, expression=expressions.Null())


@dataclass
class StringCaseNode(ExpressionNode):
    """Node for UPPER, LOWER operation"""

    expr: ExpressionNode
    case: Literal["upper", "lower"]

    @property
    def sql(self) -> Expression:
        expression = {"upper": expressions.Upper, "lower": expressions.Lower}[self.case]
        return expression(this=self.expr.sql)


@dataclass
class StringContains(ExpressionNode):
    """Node for CONTAINS operation"""

    expr: ExpressionNode
    pattern: str
    case: bool

    @property
    def sql(self) -> Expression:
        if self.case:
            return fb_expressions.Contains(
                this=self.expr.sql,
                pattern=make_literal_value(self.pattern),
            )
        return fb_expressions.Contains(
            this=expressions.Lower(this=self.expr.sql),
            pattern=expressions.Lower(this=make_literal_value(self.pattern)),
        )


@dataclass
class TrimNode(ExpressionNode):
    """Node for TRIM, LTRIM, RTRIM operations"""

    expr: ExpressionNode
    character: Optional[str]
    side: Literal["left", "right", "both"]

    @property
    def sql(self) -> Expression:
        expression_class = {
            "left": fb_expressions.LTrim,
            "right": fb_expressions.RTrim,
            "both": fb_expressions.Trim,
        }[self.side]
        if self.character:
            return expression_class(
                this=self.expr.sql, character=make_literal_value(self.character)
            )
        return expression_class(this=self.expr.sql)


@dataclass
class ReplaceNode(ExpressionNode):
    """Node for REPLACE operation"""

    expr: ExpressionNode
    pattern: str
    replacement: str

    @property
    def sql(self) -> Expression:
        return fb_expressions.Replace(
            this=self.expr.sql,
            pattern=make_literal_value(self.pattern),
            replacement=make_literal_value(self.replacement),
        )


@dataclass
class PadNode(ExpressionNode):
    """Node for LPAD, RPAD operation"""

    expr: ExpressionNode
    side: Literal["left", "right", "both"]
    length: int
    pad: str

    @staticmethod
    def _generate_pad_expression(
        str_column_expr: Expression,
        target_length_expr: Expression,
        side: Literal["left", "right"],
        pad_char: str,
    ) -> Expression:
        pad_char_expr = make_literal_value(pad_char)
        if side == "left":
            return fb_expressions.LPad(
                this=str_column_expr,
                length=target_length_expr,
                pad=pad_char_expr,
            )
        return fb_expressions.RPad(
            this=str_column_expr,
            length=target_length_expr,
            pad=pad_char_expr,
        )

    @property
    def sql(self) -> Expression:
        target_length_expr = make_literal_value(self.length)
        char_length_expr = expressions.Length(this=self.expr.sql)
        mask_expr = expressions.GTE(this=char_length_expr, expression=target_length_expr)
        if self.side in {"left", "right"}:
            pad_expr = self._generate_pad_expression(
                str_column_expr=self.expr.sql,
                target_length_expr=make_literal_value(self.length),
                side=self.side,  # type: ignore
                pad_char=self.pad,
            )
        else:
            remain_width = expressions.Paren(
                this=expressions.Sub(this=target_length_expr, expression=char_length_expr)
            )
            left_remain_width = expressions.Ceil(
                this=expressions.Div(this=remain_width, expression=make_literal_value(2))
            )
            left_length = expressions.Sub(this=target_length_expr, expression=left_remain_width)
            pad_expr = self._generate_pad_expression(
                str_column_expr=self._generate_pad_expression(
                    str_column_expr=self.expr.sql,
                    target_length_expr=left_length,
                    side="left",
                    pad_char=self.pad,
                ),
                target_length_expr=target_length_expr,
                side="right",
                pad_char=self.pad,
            )
        return expressions.If(this=mask_expr, true=self.expr.sql, false=pad_expr)


@dataclass
class SubStringNode(ExpressionNode):
    """Node for SUBSTRING operation"""

    expr: ExpressionNode
    start: int
    length: Optional[int]

    @property
    def sql(self) -> Expression:
        params = {"this": self.expr.sql, "start": make_literal_value(self.start)}
        if self.length is not None:
            params["length"] = make_literal_value(self.length)
        return expressions.Substring(**params)


@dataclass
class DatetimeExtractNode(ExpressionNode):
    """Node for extract datetime properties operation"""

    expr: ExpressionNode
    dt_property: DatetimeSupportedPropertyType

    @property
    def sql(self) -> Expression:
        params = {"this": self.dt_property, "expression": self.expr.sql}
        prop_expr = expressions.Extract(**params)
        if self.dt_property == "dayofweek":
            # pandas: Monday=0, Sunday=6; snowflake: Sunday=0, Saturday=6
            # to follow pandas behavior, add 6 then modulo 7 to perform left-shift
            return expressions.Mod(
                this=expressions.Paren(
                    this=expressions.Add(this=prop_expr, expression=make_literal_value(6))
                ),
                expression=make_literal_value(7),
            )
        return prop_expr


@dataclass
class DateDiffNode(ExpressionNode):
    """Node for date difference operation"""

    left_node: ExpressionNode
    right_node: ExpressionNode

    def with_unit(self, unit: TimedeltaSupportedUnitType) -> Expression:
        """Construct a date difference expression with provided time unit

        Parameters
        ----------
        unit : TimedeltaSupportedUnitType
            Time unit

        Returns
        -------
        Expression
        """
        output_expr = expressions.Anonymous(
            this="DATEDIFF",
            expressions=[
                expressions.Identifier(this=unit),
                self.right_node.sql,
                self.left_node.sql,
            ],
        )
        return output_expr

    @property
    def sql(self) -> Expression:
        return self.with_unit("second")


@dataclass
class TimedeltaExtractNode(ExpressionNode):
    """Node for converting Timedelta to numeric value given a unit"""

    timedelta_node: Union[TimedeltaNode, DateDiffNode]
    unit: TimedeltaSupportedUnitType

    @property
    def sql(self) -> Expression:
        if isinstance(self.timedelta_node, DateDiffNode):
            expr = self.timedelta_node.with_unit("microsecond")
            output_expr = self.convert_timedelta_unit(expr, "microsecond", self.unit)
        else:
            output_expr = self.convert_timedelta_unit(
                self.timedelta_node.sql, self.timedelta_node.unit, self.unit
            )
        return output_expr

    @classmethod
    def convert_timedelta_unit(
        cls,
        input_expr: Expression,
        input_unit: TimedeltaSupportedUnitType,
        output_unit: TimedeltaSupportedUnitType,
    ) -> Expression:
        """Create an expression that converts a timedelta column to another unit

        Parameters
        ----------
        input_expr : Expression
            Expression for the timedelta value. Should evaluate to numeric result
        input_unit : TimedeltaSupportedUnitType
            The time unit that input_expr is in
        output_unit : TimedeltaSupportedUnitType
            The desired unit to convert to

        Returns
        -------
        Expression
        """
        input_unit_milli_seconds = int(pd.Timedelta(1, unit=input_unit).total_seconds() * 1e6)
        output_unit_milli_seconds = int(pd.Timedelta(1, unit=output_unit).total_seconds() * 1e6)
        converted_expr = expressions.Div(
            this=expressions.Mul(
                this=input_expr, expression=make_literal_value(input_unit_milli_seconds)
            ),
            expression=make_literal_value(output_unit_milli_seconds),
        )
        converted_expr = expressions.Paren(this=converted_expr)
        return converted_expr


@dataclass
class TimedeltaNode(ExpressionNode):
    """Node to represent Timedelta"""

    value_expr: ExpressionNode
    unit: TimedeltaSupportedUnitType

    @property
    def sql(self) -> Expression:
        return self.value_expr.sql


@dataclass
class DateAddNode(ExpressionNode):
    """Node for date increment by timedelta operation"""

    input_date_node: ExpressionNode
    timedelta_node: Union[TimedeltaNode, DateDiffNode, ParsedExpressionNode]

    @property
    def sql(self) -> Expression:
        if isinstance(self.timedelta_node, TimedeltaNode):
            # timedelta is constructed from to_timedelta()
            date_add_args = [
                self.timedelta_node.unit,
                self.timedelta_node.sql,
                self.input_date_node.sql,
            ]
        elif isinstance(self.timedelta_node, DateDiffNode):
            # timedelta is the result of date difference
            date_add_args = [
                "microsecond",
                self.timedelta_node.with_unit("microsecond"),
                self.input_date_node.sql,
            ]
        else:
            # timedelta is a constant value
            date_add_args = [
                "second",
                self.timedelta_node.sql,
                self.input_date_node.sql,
            ]
        output_expr = expressions.Anonymous(this="DATEADD", expressions=date_add_args)
        return output_expr


@dataclass
class CountDictTransformNode(ExpressionNode):
    """Node for count dict transform operation (eg. entropy)"""

    expr: ExpressionNode
    transform_type: Literal["entropy", "most_frequent", "unique_count"]
    include_missing: bool

    @property
    def sql(self) -> Expression:
        function_name = {
            "entropy": "F_COUNT_DICT_ENTROPY",
            "most_frequent": "F_COUNT_DICT_MOST_FREQUENT",
            "unique_count": "F_COUNT_DICT_NUM_UNIQUE",
        }[self.transform_type]
        if self.include_missing:
            counts_expr = self.expr.sql
        else:
            counts_expr = expressions.Anonymous(
                this="OBJECT_DELETE",
                expressions=[self.expr.sql, make_literal_value(MISSING_VALUE_REPLACEMENT)],
            )
        output_expr = expressions.Anonymous(this=function_name, expressions=[counts_expr])
        if self.transform_type == "most_frequent":
            # The F_COUNT_DICT_MOST_FREQUENT UDF produces a VARIANT type. Cast to string to prevent
            # double quoting in the feature output ('remove' vs '"remove"')
            output_expr = expressions.Cast(this=output_expr, to=parse_one("VARCHAR"))
        return output_expr


@dataclass
class CastNode(ExpressionNode):
    """Node for casting operation"""

    expr: ExpressionNode
    new_type: Literal["int", "float", "str"]
    from_dtype: DBVarType

    @property
    def sql(self) -> Expression:
        if self.from_dtype == DBVarType.FLOAT and self.new_type == "int":
            # Casting to INTEGER performs rounding (could be up or down). Hence, apply FLOOR first
            # to mimic pandas astype(int)
            expr = expressions.Floor(this=self.expr.sql)
        elif self.from_dtype == DBVarType.BOOL and self.new_type == "float":
            # Casting to FLOAT from BOOL directly is not allowed
            expr = expressions.Cast(this=self.expr.sql, to=parse_one("INTEGER"))
        else:
            expr = self.expr.sql
        type_expr = {
            "int": parse_one("INTEGER"),
            "float": parse_one("FLOAT"),
            "str": parse_one("VARCHAR"),
        }[self.new_type]
        output_expr = expressions.Cast(this=expr, to=type_expr)
        return output_expr


@dataclass
class LagNode(ExpressionNode):
    """Node for lag operation"""

    expr: ExpressionNode
    entity_columns: list[str]
    timestamp_column: str
    offset: int

    @property
    def sql(self) -> Expression:
        partition_by = [
            expressions.Column(this=expressions.Identifier(this=col, quoted=True))
            for col in self.entity_columns
        ]
        order = expressions.Order(
            expressions=[
                expressions.Ordered(
                    this=expressions.Identifier(this=self.timestamp_column, quoted=True)
                )
            ]
        )
        output_expr = expressions.Window(
            this=expressions.Anonymous(
                this="LAG", expressions=[self.expr.sql, make_literal_value(self.offset)]
            ),
            partition_by=partition_by,
            order=order,
        )
        return output_expr


@dataclass
class BuildTileNode(TableNode):
    """Tile builder node

    This node is responsible for generating the tile building SQL for a groupby operation.
    """

    input_node: TableNode
    keys: list[str]
    value_by: str | None
    tile_specs: list[TileSpec]
    timestamp: str
    agg_func: str
    frequency: int
    is_on_demand: bool

    @property
    def sql(self) -> Expression:
        if self.is_on_demand:
            start_date_expr = InternalName.ENTITY_TABLE_START_DATE
        else:
            start_date_expr = InternalName.TILE_START_DATE_SQL_PLACEHOLDER

        start_date_epoch = f"DATE_PART(EPOCH_SECOND, CAST({start_date_expr} AS TIMESTAMP))"
        timestamp_epoch = f"DATE_PART(EPOCH_SECOND, {escape_column_name(self.timestamp)})"

        input_tiled = select(
            "*",
            f"FLOOR(({timestamp_epoch} - {start_date_epoch}) / {self.frequency}) AS tile_index",
        ).from_(self.input_node.sql_nested())

        tile_start_date = f"TO_TIMESTAMP({start_date_epoch} + tile_index * {self.frequency})"
        keys = escape_column_names(self.keys)
        if self.value_by is not None:
            keys.append(escape_column_name(self.value_by))

        if self.is_on_demand:
            groupby_keys = keys + [InternalName.ENTITY_TABLE_START_DATE.value]
        else:
            groupby_keys = keys

        groupby_sql = (
            select(
                f"{tile_start_date} AS {InternalName.TILE_START_DATE}",
                *keys,
                *[f"{spec.tile_expr} AS {spec.tile_column_name}" for spec in self.tile_specs],
            )
            .from_(input_tiled.subquery())
            .group_by("tile_index", *groupby_keys)
            .order_by("tile_index")
        )

        return groupby_sql


@dataclass
class AggregatedTilesNode(TableNode):
    """Node with tiles already aggregated

    The purpose of this node is to allow feature SQL generation to retrieve the post-aggregation
    feature transform expression. The columns_map of this node has the mapping from user defined
    feature names to internal aggregated column names. The feature expression can be obtained by
    calling get_column_expr().
    """

    @property
    def sql(self) -> Expression:
        # This will not be called anywhere
        raise NotImplementedError()


BINARY_OPERATION_NODE_TYPES = {
    NodeType.ADD,
    NodeType.SUB,
    NodeType.MUL,
    NodeType.DIV,
    NodeType.MOD,
    NodeType.EQ,
    NodeType.NE,
    NodeType.LT,
    NodeType.LE,
    NodeType.GT,
    NodeType.GE,
    NodeType.AND,
    NodeType.OR,
    NodeType.CONCAT,
    NodeType.COSINE_SIMILARITY,
    NodeType.DATE_DIFF,
    NodeType.DATE_ADD,
    NodeType.POWER,
}


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


def make_literal_value(value: Any) -> expressions.Literal | expressions.Null:
    """Create a sqlglot literal value

    Parameters
    ----------
    value : Any
        The literal value

    Returns
    -------
    expressions.Literal | expressions.Null
    """
    if isinstance(value, str):
        return expressions.Literal.string(value)
    if is_scalar_nan(value):
        return expressions.Null()
    return expressions.Literal.number(value)


def make_binary_operation_node(
    node_type: NodeType,
    input_sql_nodes: list[SQLNode],
    parameters: dict[str, Any],
) -> BinaryOp | DateDiffNode | DateAddNode:
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
    BinaryOp | DateDiffNode | DateAddNode

    Raises
    ------
    NotImplementedError
        For incompatible node types
    """
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

    node_type_to_expression_cls = {
        # Arithmetic
        NodeType.ADD: expressions.Add,
        NodeType.SUB: expressions.Sub,
        NodeType.MUL: expressions.Mul,
        NodeType.DIV: expressions.Div,
        NodeType.MOD: expressions.Mod,
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
        # String
        NodeType.CONCAT: fb_expressions.Concat,
        NodeType.COSINE_SIMILARITY: fb_expressions.CosineSim,
        NodeType.POWER: expressions.Pow,
    }

    output_node: BinaryOp | DateDiffNode | DateAddNode
    if node_type in node_type_to_expression_cls:
        expression_cls = node_type_to_expression_cls[node_type]
        output_node = BinaryOp(
            table_node=table_node,
            left_node=left_node,
            right_node=right_node,
            operation=expression_cls,
        )
    elif node_type == NodeType.DATE_DIFF:
        output_node = DateDiffNode(
            table_node=table_node,
            left_node=left_node,
            right_node=right_node,
        )
    elif node_type == NodeType.DATE_ADD:
        output_node = make_date_add_node(
            table_node=table_node, input_date_node=left_node, timedelta_node=right_node
        )
    else:
        raise NotImplementedError(f"{node_type} cannot be converted to binary operation")

    return output_node


def make_date_add_node(
    table_node: TableNode,
    input_date_node: ExpressionNode,
    timedelta_node: ExpressionNode,
) -> DateAddNode:
    """Create a DateAddNode

    Parameters
    ----------
    table_node : TableNode
        TableNode
    input_date_node : ExpressionNode
        Node for date expression
    timedelta_node : ExpressionNode
        Node for timedelta expression

    Returns
    -------
    DateAddNode
    """
    resolved_timedelta_node = resolve_project_node(timedelta_node)
    assert isinstance(resolved_timedelta_node, (TimedeltaNode, DateDiffNode, ParsedExpressionNode))
    output_node = DateAddNode(
        table_node=table_node,
        input_date_node=input_date_node,
        timedelta_node=resolved_timedelta_node,
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


def make_build_tile_node(
    input_sql_nodes: list[SQLNode], parameters: dict[str, Any], is_on_demand: bool
) -> BuildTileNode:
    """Create a BuildTileNode

    Parameters
    ----------
    input_sql_nodes : list[SQLNode]
        List of input SQL nodes
    parameters : dict[str, Any]
        Query node parameters
    is_on_demand : bool
        Whether the SQL is for on-demand tile building for historical features

    Returns
    -------
    BuildTileNode
    """
    input_node = input_sql_nodes[0]
    assert isinstance(input_node, TableNode)
    aggregator = get_aggregator(parameters["agg_func"])
    tile_specs = aggregator.tile(parameters["parent"], parameters["aggregation_id"])
    columns = (
        [InternalName.TILE_START_DATE.value]
        + parameters["keys"]
        + [spec.tile_column_name for spec in tile_specs]
    )
    columns_map = {col: expressions.Identifier(this=col, quoted=True) for col in columns}
    sql_node = BuildTileNode(
        columns_map=columns_map,
        input_node=input_node,
        keys=parameters["keys"],
        value_by=parameters["value_by"],
        tile_specs=tile_specs,
        timestamp=parameters["timestamp"],
        agg_func=parameters["agg_func"],
        frequency=parameters["frequency"],
        is_on_demand=is_on_demand,
    )
    return sql_node


def make_input_node(
    parameters: dict[str, Any],
    sql_type: SQLType,
    groupby_keys: list[str] | None = None,
) -> BuildTileInputNode | InputNode:
    """Create a SQLNode corresponding to a query graph input node

    Parameters
    ----------
    parameters : dict[str, Any]
        Query graph node parameters
    sql_type: SQLType
        Type of SQL code to generate
    groupby_keys : list[str] | None
        List of groupby keys that is used for the downstream groupby operation. This information is
        required so that only tiles corresponding to specific entities are built (vs building tiles
        using all available data). This option is only used when SQLType is BUILD_TILE_ON_DEMAND.

    Returns
    -------
    BuildTileInputNode | InputNode | SelectedEntityBuildTileInputNode
        SQLNode corresponding to the query graph input node
    """
    columns_map = {}
    for colname in parameters["columns"]:
        columns_map[colname] = expressions.Identifier(this=colname, quoted=True)
    sql_node: BuildTileInputNode | SelectedEntityBuildTileInputNode | InputNode
    feature_store = parameters["feature_store_details"]
    if sql_type == SQLType.BUILD_TILE:
        sql_node = BuildTileInputNode(
            columns_map=columns_map,
            timestamp=parameters["timestamp"],
            dbtable=parameters["table_details"],
            feature_store=feature_store,
        )
    elif sql_type == SQLType.BUILD_TILE_ON_DEMAND:
        assert groupby_keys is not None
        sql_node = SelectedEntityBuildTileInputNode(
            columns_map=columns_map,
            timestamp=parameters["timestamp"],
            dbtable=parameters["table_details"],
            feature_store=feature_store,
            entity_columns=groupby_keys,
        )
    else:
        sql_node = InputNode(
            columns_map=columns_map,
            dbtable=parameters["table_details"],
            feature_store=feature_store,
        )
    return sql_node


def make_aggregated_tiles_node(groupby_node: Node) -> AggregatedTilesNode:
    """Create a TableNode representing the aggregated tiles

    Parameters
    ----------
    groupby_node : Node
        Query graph node with groupby type

    Returns
    -------
    AggregatedTilesNode
    """
    agg_specs = AggregationSpec.from_groupby_query_node(groupby_node)
    columns_map = {}
    for agg_spec in agg_specs:
        columns_map[agg_spec.feature_name] = expressions.Identifier(
            this=agg_spec.agg_result_name, quoted=True
        )
    return AggregatedTilesNode(columns_map=columns_map)


def handle_groupby_node(
    groupby_node: Node,
    parameters: dict[str, Any],
    input_sql_nodes: list[SQLNode],
    sql_type: SQLType,
) -> BuildTileNode | AggregatedTilesNode:
    """Handle a groupby query graph node and create an appropriate SQLNode

    Parameters
    ----------
    groupby_node : Node
        Groupby query graph
    parameters : dict[str, Any]
        Query node parameters
    input_sql_nodes : list[SQLNode]
        Input SQL nodes
    sql_type : SQLType
        Type of SQL code to generate

    Returns
    -------
    BuildTileNode | AggregatedTilesNode
        Resulting SQLNode

    Raises
    ------
    NotImplementedError
        If the provided query node is not supported
    """
    sql_node: BuildTileNode | AggregatedTilesNode
    if sql_type == SQLType.BUILD_TILE:
        sql_node = make_build_tile_node(input_sql_nodes, parameters, is_on_demand=False)
    elif sql_type == SQLType.BUILD_TILE_ON_DEMAND:
        sql_node = make_build_tile_node(input_sql_nodes, parameters, is_on_demand=True)
    elif sql_type == SQLType.GENERATE_FEATURE:
        sql_node = make_aggregated_tiles_node(groupby_node)
    else:
        raise NotImplementedError(f"SQLNode not implemented for {groupby_node}")
    return sql_node


def make_conditional_node(input_sql_nodes: list[SQLNode], node: Node) -> Conditional:
    """Create a Conditional node

    Parameters
    ----------
    input_sql_nodes : list[SQLNode]
        Input SQL nodes
    node : Node
        Query graph node

    Returns
    -------
    Conditional
    """
    assert len(input_sql_nodes) == 2
    parameters = node.parameters.dict()

    series_node = input_sql_nodes[0]
    mask = input_sql_nodes[1]
    value = parameters["value"]
    assert isinstance(series_node, ExpressionNode)
    assert isinstance(mask, ExpressionNode)
    input_table_node = series_node.table_node

    sql_node = Conditional(
        table_node=input_table_node, series_node=series_node, mask=mask, value=value
    )
    return sql_node


SUPPORTED_EXPRESSION_NODE_TYPES = {
    NodeType.IS_NULL,
    NodeType.LENGTH,
    NodeType.TRIM,
    NodeType.REPLACE,
    NodeType.PAD,
    NodeType.STR_CASE,
    NodeType.STR_CONTAINS,
    NodeType.SUBSTRING,
    NodeType.DT_EXTRACT,
    NodeType.NOT,
    NodeType.COUNT_DICT_TRANSFORM,
    NodeType.CAST,
    NodeType.LAG,
    NodeType.TIMEDELTA_EXTRACT,
    NodeType.TIMEDELTA,
    NodeType.SQRT,
    NodeType.ABS,
    NodeType.FLOOR,
    NodeType.CEIL,
    NodeType.LOG,
    NodeType.EXP,
}


def make_timedelta_extract_node(
    input_expr_node: ExpressionNode, parameters: dict[str, Any]
) -> ExpressionNode:
    """Create a SQLNode for extracting timedelta as a numeric value

    Parameters
    ----------
    input_expr_node : ExpressionNode
        Node for the timedelta value
    parameters: dict[str, Any]
        Query node parameters

    Returns
    -------
    ExpressionNode
    """
    # Need to retrieve the original DateDiffNode to rewrite the expression with new unit
    resolved_expr_node = resolve_project_node(input_expr_node)
    assert isinstance(resolved_expr_node, (DateDiffNode, TimedeltaNode))
    sql_node = TimedeltaExtractNode(
        table_node=input_expr_node.table_node,
        timedelta_node=resolved_expr_node,
        unit=parameters["property"],
    )
    return sql_node


def make_expression_node(
    input_sql_nodes: list[SQLNode], node_type: NodeType, parameters: dict[str, Any]
) -> ExpressionNode:
    """Create an Expression node

    Parameters
    ----------
    input_sql_nodes : list[SQLNode]
        Input SQL nodes
    node_type : NodeType
        Query graph node type
    parameters: dict[str, Any]
        Query node parameters

    Returns
    -------
    ExpressionNode

    Raises
    ------
    NotImplementedError
        if the query graph node type is not supported
    """
    # pylint: disable=too-many-branches
    input_expr_node = input_sql_nodes[0]
    assert isinstance(input_expr_node, ExpressionNode)
    table_node = input_expr_node.table_node
    sql_node: ExpressionNode

    # Some node types can be handled identically given the correct sqlglot expression
    node_type_to_expression_cls = {
        NodeType.SQRT: expressions.Sqrt,
        NodeType.ABS: expressions.Abs,
        NodeType.FLOOR: expressions.Floor,
        NodeType.CEIL: expressions.Ceil,
        NodeType.NOT: expressions.Not,
        NodeType.LENGTH: expressions.Length,
        NodeType.LOG: expressions.Ln,
        NodeType.EXP: expressions.Exp,
    }
    if node_type in node_type_to_expression_cls:
        cls = node_type_to_expression_cls[node_type]
        sql_node = UnaryOp(table_node=table_node, expr=input_expr_node, operation=cls)
    elif node_type == NodeType.IS_NULL:
        sql_node = IsNullNode(table_node=table_node, expr=input_expr_node)
    elif node_type == NodeType.STR_CASE:
        sql_node = StringCaseNode(
            table_node=table_node,
            expr=input_expr_node,
            case=parameters["case"],
        )
    elif node_type == NodeType.TRIM:
        sql_node = TrimNode(
            table_node=table_node,
            expr=input_expr_node,
            character=parameters["character"],
            side=parameters["side"],
        )
    elif node_type == NodeType.REPLACE:
        sql_node = ReplaceNode(
            table_node=table_node,
            expr=input_expr_node,
            pattern=parameters["pattern"],
            replacement=parameters["replacement"],
        )
    elif node_type == NodeType.PAD:
        sql_node = PadNode(
            table_node=table_node,
            expr=input_expr_node,
            side=parameters["side"],
            length=parameters["length"],
            pad=parameters["pad"],
        )
    elif node_type == NodeType.STR_CONTAINS:
        sql_node = StringContains(
            table_node=table_node,
            expr=input_expr_node,
            pattern=parameters["pattern"],
            case=parameters["case"],
        )
    elif node_type == NodeType.SUBSTRING:
        sql_node = SubStringNode(
            table_node=table_node,
            expr=input_expr_node,
            start=parameters["start"],
            length=parameters["length"],
        )
    elif node_type == NodeType.DT_EXTRACT:
        sql_node = DatetimeExtractNode(
            table_node=table_node,
            expr=input_expr_node,
            dt_property=parameters["property"],
        )
    elif node_type == NodeType.TIMEDELTA_EXTRACT:
        sql_node = make_timedelta_extract_node(input_expr_node, parameters)

    elif node_type == NodeType.TIMEDELTA:
        sql_node = TimedeltaNode(
            table_node=table_node, value_expr=input_expr_node, unit=parameters["unit"]
        )

    elif node_type == NodeType.COUNT_DICT_TRANSFORM:
        sql_node = CountDictTransformNode(
            table_node=table_node,
            expr=input_expr_node,
            transform_type=parameters["transform_type"],
            include_missing=parameters.get("include_missing", True),
        )
    elif node_type == NodeType.CAST:
        sql_node = CastNode(
            table_node=table_node,
            expr=input_expr_node,
            new_type=parameters["type"],
            from_dtype=parameters["from_dtype"],
        )
    elif node_type == NodeType.LAG:
        sql_node = LagNode(
            table_node=table_node,
            expr=input_expr_node,
            entity_columns=parameters["entity_columns"],
            timestamp_column=parameters["timestamp_column"],
            offset=parameters["offset"],
        )
    else:
        raise NotImplementedError(f"Unexpected node type: {node_type}")
    return sql_node
