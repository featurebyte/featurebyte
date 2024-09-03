"""
Common helpers and data structures for feature SQL generation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Sequence, Tuple, Union

from bson import ObjectId
from sqlglot import expressions
from sqlglot.expressions import Expression, select

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.dialects import get_dialect_from_source_type

REQUEST_TABLE_NAME = "REQUEST_TABLE"

# Replacement value for missing values where they should be preserved. Use typically in dictionary
# related operations which discard missing keys by default.
MISSING_VALUE_REPLACEMENT = "__MISSING__"

CteStatement = Tuple[Union[str, Expression], Expression]
CteStatements = Sequence[CteStatement]


def construct_cte_sql(
    cte_statements: CteStatements,
) -> expressions.Select:
    """Construct CTEs section of a SQL code

    Parameters
    ----------
    cte_statements : CteStatements
        List of CTE statements

    Returns
    -------
    expression.Select
    """
    cte_expr = select()
    for table_name, table_expr in cte_statements:
        cte_expr = cte_expr.with_(table_name, table_expr)
    return cte_expr


def quoted_identifier(column_name: str) -> Expression:
    """Construct a quoted Identifier

    Parameters
    ----------
    column_name : str
        Column name

    Returns
    -------
    Expression
    """
    return expressions.Identifier(this=column_name, quoted=True)


def get_qualified_column_identifier(
    column_name: str,
    table: str,
    quote_table: bool = False,
    quote_column: bool = True,
) -> Expression:
    """
    Get a qualified column name with a table alias prefix

    Parameters
    ----------
    column_name: str
        Column name
    table: str
        Table prefix to add to the column name
    quote_table : bool
        Whether to enclose the table prefix in quotes
    quote_column : bool
        Whether to enclose the column name in quotes

    Returns
    -------
    Expression
    """
    if quote_table:
        table = quoted_identifier(table)  # type: ignore[assignment]
    if quote_column:
        column_name = quoted_identifier(column_name)  # type: ignore[assignment]
    expr = expressions.Column(this=column_name, table=table)
    return expr


def get_fully_qualified_table_name(
    table_details_dict: Union[Dict[str, str], Dict[str, str | None]], alias: Optional[str] = None
) -> Expression:
    """
    Get an expression for fully qualified table name

    Parameters
    ----------
    table_details_dict: Union[Dict[str, str], Dict[str, str | None]]
        Table details dictionary
    alias: Optional[str]
        Table alias

    Returns
    -------
    Expression
    """
    schema_name = table_details_dict.get("schema_name")
    database_name = table_details_dict.get("database_name")
    table_name = table_details_dict.get("table_name")
    assert table_name is not None
    # expressions.Table's notation for three part fully qualified name is
    # {catalog}.{db}.{this}
    return expressions.Table(
        this=quoted_identifier(table_name),
        db=quoted_identifier(schema_name) if schema_name else None,
        catalog=quoted_identifier(database_name) if database_name else None,
        alias=alias,
    )


def get_fully_qualified_function_call(
    database_name: str,
    schema_name: str,
    function_name: str,
    args: Sequence[Expression],
) -> Expression:
    """
    Get an expression for calling a UDF function using fully qualified name

    Parameters
    ----------
    database_name: str
        Database name of the function
    schema_name: str
        Schema name of the function
    function_name: str
        Function name
    args: Sequence[Expression]
        Arguments to the function

    Returns
    -------
    Expression
    """
    return expressions.Dot(
        this=expressions.Dot(
            this=expressions.Var(this=quoted_identifier(database_name)),
            expression=expressions.Var(this=quoted_identifier(schema_name)),
        ),
        expression=expressions.Anonymous(this=function_name, expressions=args),
    )


def get_column_expr_and_name(
    col_expr: expressions.Column,
) -> Tuple[expressions.Expression, Optional[str]]:
    """
    Get the inner expression and name given a Column expression. Typically used when rewriting a
    select statement.

    Parameters
    ----------
    col_expr: expressions.Column
        Column expression

    Returns
    -------
    Tuple[expressions.Expression, Optional[str]]
    """
    if isinstance(col_expr, expressions.Alias):
        name = col_expr.alias
        col_expr = col_expr.this
    elif col_expr.name:
        name = col_expr.name
    else:
        name = None
    return col_expr, name


def sql_to_string(sql_expr: Expression, source_type: SourceType) -> str:
    """Convert a SQL expression to text given the source type

    Parameters
    ----------
    sql_expr : Expression
        SQL expression object
    source_type : SourceType
        The type of the database engine which will be used to determine the SQL dialect

    Returns
    -------
    str
    """
    return sql_expr.sql(dialect=get_dialect_from_source_type(source_type), pretty=True)


def apply_serving_names_mapping(serving_names: list[str], mapping: dict[str, str]) -> list[str]:
    """Apply user provided mapping to transform the default serving names

    Applicable to the serving_names attribute in TileGenSql and AggregationSpec

    Parameters
    ----------
    serving_names : list[str]
        List of original serving names
    mapping : dict[str, str]
        Mapping from original serving name to new serving name

    Returns
    -------
    list[str]
        Mapped serving names
    """
    updated_serving_names = []
    for serving_name in serving_names:
        updated_serving_names.append(mapping.get(serving_name, serving_name))
    return updated_serving_names


class SQLType(Enum):
    """Type of SQL code corresponding to different operations"""

    BUILD_TILE = "build_tile"
    BUILD_TILE_ON_DEMAND = "build_tile_on_demand"
    MATERIALIZE = "materialize"
    AGGREGATION = "aggregation"
    POST_AGGREGATION = "post_aggregation"


@dataclass
class EventTableTimestampFilter:
    """
    Information about the timestamp filter to be applied when selecting from EventTable

    timestamp_column_name: str
        Name of the timestamp column
    event_table_id: ObjectId
        Id of the EventTable. Only EventTable matching this id should be filtered.
    """

    timestamp_column_name: str
    event_table_id: ObjectId
    start_timestamp_placeholder_name: Optional[str] = None
    end_timestamp_placeholder_name: Optional[str] = None
    to_cast_placeholders: Optional[bool] = True


@dataclass
class OnDemandEntityFilter:
    """
    Information about the filter that can be applied to InputTable based on the entity values that
    appear in the observation table. Only applies for on-demand tile generation for historical
    features.

    table_id: ObjectId
        Id of the table to be filtered
    entity_columns: list[str]
        Entity column names
    """

    table_id: ObjectId
    entity_columns: list[str]
    table_columns: list[str]


@dataclass
class OnDemandEntityFilters:
    """
    Collection of on demand entity filters
    """

    entity_columns: list[str]
    mapping: dict[ObjectId, OnDemandEntityFilter] = field(default_factory=dict)

    def add_entity_column(
        self, table_id: ObjectId, entity_column_name: str, table_column_name: str
    ) -> None:
        """
        Add an entity column to the collection

        Parameters
        ----------
        table_id: ObjectId
            Table id
        entity_column_name: str
            Entity column name in the entity table
        table_column_name: str
            Column name in the source table that corresponds to the entity
        """
        if table_id not in self.mapping:
            self.mapping[table_id] = OnDemandEntityFilter(
                table_id, entity_columns=[], table_columns=[]
            )
        entity_filter = self.mapping[table_id]
        entity_filter.entity_columns.append(entity_column_name)
        entity_filter.table_columns.append(table_column_name)
