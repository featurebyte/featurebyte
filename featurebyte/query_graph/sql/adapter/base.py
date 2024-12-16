"""
Base class for SQL adapters
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from numpy import format_float_positional
from sqlglot import expressions
from sqlglot.expressions import Expression, Select, alias_, select
from typing_extensions import Literal

from featurebyte.enum import DBVarType, InternalName
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    MISSING_VALUE_REPLACEMENT,
    get_fully_qualified_table_name,
    get_qualified_column_identifier,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.typing import DatetimeSupportedPropertyType

FB_QUALIFY_CONDITION_COLUMN = "__fb_qualify_condition_column"


@dataclass
class VectorAggColumn:
    """
    Represents a set of parameters that produces one output column in a groupby statement
    """

    aggr_expr: Select
    result_name: str


class BaseAdapter(ABC):
    """
    Helper class to generate engine specific SQL expressions
    """

    TABLESAMPLE_SUPPORTS_VIEW = True
    TIMEZONE_DATE_FORMAT_EXPRESSIONS: List[str] = []

    def __init__(self, source_info: SourceInfo):
        self.source_info = source_info
        self.source_type = source_info.source_type

    @classmethod
    @abstractmethod
    def to_epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        """
        Expression to convert a timestamp to epoch second

        Parameters
        ----------
        timestamp_expr : Expression
            Input expression

        Returns
        -------
        Expression
        """

    @classmethod
    @abstractmethod
    def from_epoch_seconds(cls, timestamp_epoch_expr: Expression) -> Expression:
        """
        Expression to convert epoch second to timestamp

        Parameters
        ----------
        timestamp_epoch_expr: Expression
            Input expression

        Returns
        -------
        Expression
        """

    @classmethod
    @abstractmethod
    def str_trim(
        cls, expr: Expression, character: Optional[str], side: Literal["left", "right", "both"]
    ) -> Expression:
        """
        Expression to trim leading and / or trailing characters from string

        Parameters
        ----------
        expr : Expression
            Expression of the string input to be manipulated
        character : Optional[str]
            Character to trim, default is whitespace
        side : Literal["left", "right", "both"]
            The side of the string to be trimmed

        Returns
        -------
        Expression
        """

    @classmethod
    def str_contains(cls, expr: Expression, pattern: str) -> Expression:
        """
        Expression to check if string contains a pattern

        Parameters
        ----------
        expr: Expression
            String expression to check if it contains a pattern
        pattern: str
            Pattern to check if it is contained in the string

        Returns
        ------
        Expression
        """
        return expressions.Anonymous(
            this="CONTAINS", expressions=[expr, make_literal_value(pattern)]
        )

    @classmethod
    @abstractmethod
    def adjust_dayofweek(cls, extracted_expr: Expression) -> Expression:
        """
        Expression to adjust day of week to have consistent result as pandas

        Parameters
        ----------
        extracted_expr : Expression
            Expression representing day of week calculated by the EXTRACT function

        Returns
        -------
        Expression
        """

    @classmethod
    @abstractmethod
    def dateadd_second(cls, quantity_expr: Expression, timestamp_expr: Expression) -> Expression:
        """
        Expression to perform DATEADD using second as the time unit. Use this only when there is no
        sub-second components in the quantity.

        Parameters
        ----------
        quantity_expr : Expression
            Number of microseconds to add to the timestamp
        timestamp_expr : Expression
            Expression for the timestamp

        Returns
        -------
        Expression
        """

    @classmethod
    @abstractmethod
    def dateadd_microsecond(
        cls, quantity_expr: Expression, timestamp_expr: Expression
    ) -> Expression:
        """
        Expression to perform DATEADD using microsecond as the time unit

        Parameters
        ----------
        quantity_expr : Expression
            Number of microseconds to add to the timestamp
        timestamp_expr : Expression
            Expression for the timestamp

        Returns
        -------
        Expression
        """

    @classmethod
    def datediff_microsecond(
        cls, timestamp_expr_1: Expression, timestamp_expr_2: Expression
    ) -> Expression:
        """
        Expression to perform DATEDIFF using microsecond as the time unit

        This calculates:

        timestamp_expr_2 - timestamp_expr_1

        Parameters
        ----------
        timestamp_expr_1: Expression
            Expression for the first timestamp
        timestamp_expr_2 : Expression
            Expression for the second timestamp

        Returns
        -------
        Expression
        """
        return expressions.Anonymous(
            this="DATEDIFF",
            expressions=[
                expressions.Identifier(this="microsecond"),
                timestamp_expr_1,
                timestamp_expr_2,
            ],
        )

    @classmethod
    def get_datetime_extract_property(cls, property_type: DatetimeSupportedPropertyType) -> str:
        """
        Get the property name for the datetime extract function

        Parameters
        ----------
        property_type : DatetimeSupportedPropertyType
            Datetime property type

        Returns
        -------
        str
        """
        return str(property_type)

    @classmethod
    def construct_key_value_aggregation_sql(
        cls,
        point_in_time_column: Optional[str],
        serving_names: list[str],
        value_by: str,
        agg_result_names: list[str],
        inner_agg_result_names: list[str],
        inner_agg_expr: expressions.Select,
    ) -> expressions.Select:
        """
        Aggregate per category values into key value pairs

        # noqa: DAR103

        Input:

        --------------------------------------
        POINT_IN_TIME  ENTITY  CATEGORY  VALUE
        --------------------------------------
        2022-01-01     C1      K1        1
        2022-01-01     C1      K2        2
        2022-01-01     C2      K3        3
        2022-01-01     C3      K1        4
        ...
        --------------------------------------

        Output:

        -----------------------------------------
        POINT_IN_TIME  ENTITY  VALUE_AGG
        -----------------------------------------
        2022-01-01     C1      {"K1": 1, "K2": 2}
        2022-01-01     C2      {"K2": 3}
        2022-01-01     C3      {"K1": 4}
        ...
        -----------------------------------------

        Parameters
        ----------
        point_in_time_column : Optional[str]
            Point in time column name
        serving_names : list[str]
            List of serving name columns
        value_by : str | None
            Optional category parameter for the groupby operation
        agg_result_names : list[str]
            Column names of the aggregated results
        inner_agg_result_names : list[str]
            Column names of the intermediate aggregation result names (one value per category - this
            is to be used as the values in the aggregated key-value pairs)
        inner_agg_expr : expressions.Query:
            Query that produces the intermediate aggregation result

        Returns
        -------
        str
        """
        inner_alias = "INNER_"

        if point_in_time_column:
            outer_group_by_keys = [f"{inner_alias}.{quoted_identifier(point_in_time_column).sql()}"]
        else:
            outer_group_by_keys = []
        for serving_name in serving_names:
            outer_group_by_keys.append(f"{inner_alias}.{quoted_identifier(serving_name).sql()}")

        category_col = get_qualified_column_identifier(value_by, inner_alias)

        # Cast type to string first so that integer can be represented nicely ('{"0": 7}' vs
        # '{"0.00000": 7}')
        category_col_casted = expressions.Cast(
            this=category_col, to=expressions.DataType.build("TEXT")
        )

        # Replace missing category values since OBJECT_AGG ignores keys that are null
        category_filled_null = expressions.Case(
            ifs=[
                expressions.If(
                    this=expressions.Is(this=category_col, expression=expressions.Null()),
                    true=make_literal_value(MISSING_VALUE_REPLACEMENT),
                )
            ],
            default=category_col_casted,
        )

        object_agg_exprs = [
            alias_(
                cls.object_agg(
                    key_column=category_filled_null,
                    value_column=get_qualified_column_identifier(
                        inner_agg_result_name, inner_alias
                    ),
                ),
                alias=agg_result_name,
                quoted=True,
            )
            for inner_agg_result_name, agg_result_name in zip(
                inner_agg_result_names, agg_result_names
            )
        ]
        agg_expr = (
            select(*outer_group_by_keys, *object_agg_exprs)
            .from_(inner_agg_expr.subquery(alias=inner_alias))
            .group_by(*outer_group_by_keys)
        )
        return agg_expr

    @classmethod
    @abstractmethod
    def object_agg(cls, key_column: str | Expression, value_column: str | Expression) -> Expression:
        """
        Construct a OBJECT_AGG expression that combines a key column and a value column in to a
        dictionary column

        Parameters
        ----------
        key_column: str | Expression
            Name or expression for the key column
        value_column: str | Expression
            Name or expression for the value column

        Returns
        -------
        Expression
        """

    @classmethod
    @abstractmethod
    def get_physical_type_from_dtype(cls, dtype: DBVarType) -> str:
        """
        Get the database specific type name given a DBVarType when creating tables on the data
        warehouse (e.g. tile tables, online store tables)

        Parameters
        ----------
        dtype : DBVarType
            Data type

        Returns
        -------
        str
        """

    @classmethod
    @abstractmethod
    def object_keys(cls, dictionary_expression: Expression) -> Expression:
        """
        Gets the keys for an object of a dictionary expression.

        Parameters
        ----------
        dictionary_expression: Expression
            The Expression that should get a dictionary

        Returns
        -------
        Expression
            Expression that returns the object keys
        """

    @classmethod
    @abstractmethod
    def in_array(cls, input_expression: Expression, array_expression: Expression) -> Expression:
        """
        Checks whether the input is inside an array.

        Parameters
        ----------
        input_expression: Expression
            Input expression
        array_expression: Expression
            Array expression

        Returns
        -------
        Expression
            Expression that checks whether the input is in the array
        """

    @classmethod
    @abstractmethod
    def is_string_type(cls, column_expr: Expression) -> Expression:
        """
        Check whether the value of the input column is string type or not

        Parameters
        ----------
        column_expr: Expression
            Column expression

        Returns
        -------
        Expression
            Boolean column to indicate whether the value is string type or not
        """

    @abstractmethod
    def get_value_from_dictionary(
        self, dictionary_expression: Expression, key_expression: Expression
    ) -> Expression:
        """
        Get the value from a dictionary based on a key provided.

        Parameters
        ----------
        dictionary_expression: Expression
            expression that corresponds to the dictionary value
        key_expression: Expression
            expression that corresponds to the key we want to look up

        Returns
        -------
        Expression
            expression which returns the value for a key provided
        """

    @classmethod
    @abstractmethod
    def convert_to_utc_timestamp(cls, timestamp_expr: Expression) -> Expression:
        """
        Expression to convert timestamp to UTC time

        Parameters
        ----------
        timestamp_expr : Expression
            Expression for the timestamp

        Returns
        -------
        Expression
        """

    @classmethod
    def is_qualify_clause_supported(cls) -> bool:
        """
        Check whether the database supports the qualify clause

        Returns
        -------
        bool
        """
        return True

    @classmethod
    @abstractmethod
    def current_timestamp(cls) -> Expression:
        """
        Expression to get the current timestamp

        Returns
        -------
        Expression
        """

    @classmethod
    @abstractmethod
    def escape_quote_char(cls, query: str) -> str:
        """
        Escape the quote character in the query

        Parameters
        ----------
        query : str
            Query to escape

        Returns
        -------
        str
        """

    @classmethod
    def tablesample(cls, select_expr: Select, sample_percent: float) -> Select:
        """
        Expression to get a sample of the table

        Parameters
        ----------
        select_expr : Select
            Select expression
        sample_percent : float
            Sample percentage. This is a number between 0 to 100.

        Returns
        -------
        Select
        """
        # Nesting the query is required to handle the case when select_expr is a complex view
        # involving operations like joins
        nested_select_expr = select("*").from_(select_expr.subquery())

        # Need to perform syntax tree surgery this way since TABLESAMPLE needs to be attached to the
        # FROM clause so that the result is still a SELECT expression. This way we can do things
        # like limit(), subquery() etc on the result.
        table_sample_expr = expressions.TableSample(
            percent=expressions.Literal(
                this=format_float_positional(sample_percent, trim="-"), is_string=False
            )
        )
        nested_select_expr.args["from"].args["this"].set("sample", table_sample_expr)

        return nested_select_expr

    @classmethod
    def random_sample(
        cls,
        select_expr: Select,
        desired_row_count: int,
        total_row_count: int,
        seed: int,
        sort_by_prob: bool = True,
    ) -> Select:
        """
        Construct query to randomly sample some number of rows from a table

        Parameters
        ----------
        select_expr: Select
            Table to sample from
        desired_row_count: int
            Desired number of rows after sampling
        total_row_count: int
            Total number of rows in the table
        seed: int
            Random seed
        sort_by_prob: bool
            Whether to sort sampled result by the random probability to correct for oversampling
            bias. Can be expensive on large samples.

        Returns
        -------
        Select
        """
        if total_row_count == 0:
            return select_expr
        probability = desired_row_count / total_row_count * 1.5
        original_cols = [
            quoted_identifier(col_expr.alias or col_expr.name)
            for (column_idx, col_expr) in enumerate(select_expr.expressions)
        ]
        prob_expr = alias_(
            cls.get_uniform_distribution_expr(seed),
            alias="prob",
            quoted=True,
        )
        sampled_expr_with_prob = select(prob_expr, *original_cols).from_(select_expr.subquery())
        output = (
            select(*original_cols)
            .from_(sampled_expr_with_prob.subquery())
            .where(
                expressions.LTE(
                    this=quoted_identifier("prob"), expression=make_literal_value(probability)
                )
            )
            .limit(desired_row_count)
        )
        if sort_by_prob:
            output = output.order_by(quoted_identifier("prob"))
        return output

    @classmethod
    @abstractmethod
    def get_uniform_distribution_expr(cls, seed: int) -> Expression:
        """
        Construct an expression that returns a random number uniformly distributed between 0 and 1

        Parameters
        ----------
        seed: int
            Random seed

        Returns
        -------
        Expression
        """

    @classmethod
    def create_table_as(
        cls,
        table_details: TableDetails,
        select_expr: Select | str,
        kind: Literal["TABLE", "VIEW"] = "TABLE",
        partition_keys: list[str] | None = None,
        replace: bool = False,
        exists: bool = False,
    ) -> Expression:
        """
        Construct query to create a table using a select statement

        Parameters
        ----------
        table_details: TableDetails
            TableDetails of the table to be created
        select_expr: Select | str
            Select expression
        kind: Literal["TABLE", "VIEW"]
            Kind of table to create
        partition_keys: list[str] | None
            Partition keys
        replace: bool
            Whether to replace the table if exists
        exists: bool
            Whether to create the table only if it doesn't exist

        Returns
        -------
        Expression
        """
        destination_expr = get_fully_qualified_table_name(table_details.model_dump())
        return expressions.Create(
            this=expressions.Table(this=destination_expr),
            kind=kind,
            expression=select_expr,
            replace=replace,
            exists=exists,
        )

    @classmethod
    def filter_with_window_function(
        cls, select_expr: Select, column_names: list[str], condition: Expression
    ) -> Select:
        """
        Construct query to filter with window function

        Parameters
        ----------
        select_expr: Select
            Select expression
        column_names: list[str]
            List of column names
        condition: Expression
            Filter condition expression which involves a window function

        Returns
        -------
        Select
        """
        if cls.is_qualify_clause_supported():
            select_expr = select_expr.copy()
            select_expr.args["qualify"] = expressions.Qualify(this=condition)
        else:
            select_expr = select_expr.select(
                expressions.alias_(condition, alias=FB_QUALIFY_CONDITION_COLUMN, quoted=True)
            )
            select_expr = (
                select(*[quoted_identifier(column_name) for column_name in column_names])
                .from_(select_expr.subquery())
                .where(quoted_identifier(FB_QUALIFY_CONDITION_COLUMN))
            )
        return select_expr

    @classmethod
    def any_value(cls, expr: Expression) -> Expression:
        """
        Expression for the aggregation function that returns any value in a group

        Parameters
        ----------
        expr : Expression
            Expression to be aggregated

        Returns
        -------
        Expression
        """
        return expressions.Anonymous(this="ANY_VALUE", expressions=[expr])

    @classmethod
    def online_store_pivot_prepare_value_column(
        cls,
        dtype: DBVarType,
    ) -> Expression:
        """
        Prepare the online store value column for pivot query

        Parameters
        ----------
        dtype: DBVarType
            Data type of the value column

        Returns
        -------
        Expression
        """
        _ = dtype
        return quoted_identifier(InternalName.ONLINE_STORE_VALUE_COLUMN.value)

    @classmethod
    def online_store_pivot_aggregation_func(cls, value_column_expr: Expression) -> Expression:
        """
        Aggregation function for online store pivot query

        Parameters
        ----------
        value_column_expr: Expression
            Expression for the value column to be aggregated

        Returns
        -------
        Expression
        """
        return cls.any_value(value_column_expr)

    @classmethod
    def online_store_pivot_finalise_value_column(
        cls,
        agg_result_name: str,
        dtype: DBVarType,
    ) -> Expression:
        """
        Finalise the online store value column after pivot query

        Parameters
        -----------
        agg_result_name: str
            Name of the aggregation result column after pivot query
        dtype: DBVarType
            Original data type of the aggregation result column

        Returns
        -------
        Expression
        """
        _ = dtype
        return expressions.Identifier(this=f"{agg_result_name}", quoted=True)

    @classmethod
    def online_store_pivot_finalise_serving_name(cls, serving_name: str) -> Expression:
        """
        Finalise the online store serving name after pivot query

        Parameters
        ----------
        serving_name: str
            Serving name

        Returns
        -------
        Expression
        """
        return quoted_identifier(serving_name)

    @classmethod
    def group_by(
        cls,
        input_expr: Select,
        select_keys: List[Expression],
        agg_exprs: List[Expression],
        keys: List[Expression],
        vector_aggregate_columns: Optional[List[VectorAggColumn]] = None,
        quote_vector_agg_aliases: bool = True,
    ) -> Select:
        """
        Construct query to group by.

        Parameters
        ----------
        input_expr: Select
            Input Select expression. This will typically contain the base table data that we are querying from, and
            will be performing the groupby over.
        select_keys: List[Expression]
            List of select keys. These are keys that typically correspond to the group by keys, as we will often
            perform joins with these keys at a later point with other tables.
        agg_exprs: List[Expression]
            List of aggregation expressions. These are typically aggregation functions that have been applied on a
            column already, and potentially with an alias. An input might be something like ["sum(col1) as sum_col_1"].
        keys: List[Expression]
            List of keys. These keys refer to the columns that we want to group by.
        vector_aggregate_columns: Optional[List[Expression]]
            List of vector aggregate expressions. This should only be used if special handling is required to join
            vector aggregate functions, and that they're not usable as a normal function. This param is a no-op
            by default, and will only be used by specific data warehouses.
        quote_vector_agg_aliases: bool
            Whether to quote the vector aggregate aliases.

        Returns
        -------
        Select
        """
        _ = vector_aggregate_columns, quote_vector_agg_aliases
        return input_expr.select(*select_keys, *agg_exprs, copy=False).group_by(*keys, copy=False)

    @classmethod
    @abstractmethod
    def get_percentile_expr(cls, input_expr: Expression, quantile: float) -> Expression:
        """
        Get the percentile expression

        Parameters
        ----------
        input_expr: Expression
            Column expression to use for percentile expression
        quantile: float
            Quantile to use for percentile expression

        Returns
        -------
        Expression
        """

    @classmethod
    def radian_expr(cls, expr: Expression) -> Expression:
        """
        Construct an expression to convert degrees to radians

        Parameters
        ----------
        expr: Expression
            Expression representing degrees

        Returns
        -------
        Expression
        """
        return expressions.Anonymous(this="RADIANS", expressions=[expr])

    @staticmethod
    def _square_expr(expr: Expression) -> Expression:
        return expressions.Pow(this=expr, expression=make_literal_value(2))

    @staticmethod
    def _asin_expr(expr: Expression) -> Expression:
        return expressions.Anonymous(this="ASIN", expressions=[expr])

    @staticmethod
    def _cos_expr(expr: Expression) -> Expression:
        return expressions.Anonymous(this="COS", expressions=[expr])

    @classmethod
    def haversine(
        cls,
        lat_expr_1: Expression,
        lon_expr_1: Expression,
        lat_expr_2: Expression,
        lon_expr_2: Expression,
    ) -> Expression:
        """
        Construct an expression that contains the haversine distances between two points.

        By default, we use the formula defined as follows.

        D(x,y) = 2 * arcsin( sqrt( sin^2( (lat1 - lat2) / 2 ) + cos(lat1) * cos(lat2) * sin^2( (lon1 - lon2) / 2 ) ) ) * 6371

        6371 is the earth's radius.

        The SQL version looks like

        2 * ASIN(
            SQRT(
                POW(
                    SIN((RADIANS(TAB.LAT_1) - RADIANS(TAB.LAT_2)) / 2), 2
                )
                + COS(RADIANS(TAB.LAT_1))
                * COS(RADIANS(TAB.LAT_2))
                * POW(
                    SIN((RADIANS(TAB.LON_1) - RADIANS(TAB.LON_2)) / 2), 2
                )
            )
        ) * 6371

        Parameters
        ----------
        lat_expr_1: Expression
            Expression representing the latitude of the first point
        lon_expr_1: Expression
            Expression representing the longitude of the first point
        lat_expr_2: Expression
            Expression representing the latitude of the second point
        lon_expr_2: Expression
            Expression representing the longitude of the second point

        Returns
        -------
        Expression
        """
        radian_lat_1_expr = cls.radian_expr(lat_expr_1)
        radian_lon_1_expr = cls.radian_expr(lon_expr_1)
        radian_lat_2_expr = cls.radian_expr(lat_expr_2)
        radian_lon_2_expr = cls.radian_expr(lon_expr_2)
        pow_sin_lat_expr = cls._square_expr(
            expressions.Anonymous(
                this="SIN",
                expressions=[
                    expressions.Div(
                        this=expressions.paren(
                            expressions.Sub(this=radian_lat_1_expr, expression=radian_lat_2_expr)
                        ),
                        expression=make_literal_value(2),
                    )
                ],
            )
        )
        pow_sin_lon_expr = cls._square_expr(
            expressions.Anonymous(
                this="SIN",
                expressions=[
                    expressions.Div(
                        this=expressions.paren(
                            expressions.Sub(this=radian_lon_1_expr, expression=radian_lon_2_expr)
                        ),
                        expression=make_literal_value(2),
                    )
                ],
            )
        )
        mult_expr = expressions.Mul(
            this=cls._cos_expr(radian_lat_1_expr), expression=cls._cos_expr(radian_lat_2_expr)
        )
        mult_expr = expressions.Mul(this=mult_expr, expression=pow_sin_lon_expr)
        sqrt_expr = expressions.Sqrt(
            this=expressions.Add(this=pow_sin_lat_expr, expression=mult_expr)
        )
        asin_expr = cls._asin_expr(sqrt_expr)
        mult_by_2_expr = expressions.Mul(this=make_literal_value(2), expression=asin_expr)
        return expressions.Mul(this=mult_by_2_expr, expression=make_literal_value(6371))

    @classmethod
    def alter_table_add_columns(
        cls,
        table: expressions.Table,
        columns: List[expressions.ColumnDef],
    ) -> str:
        """
        Generate a query to add columns to an existing table

        Query is formatted manually because the current version of sqlglot doesn't generate the
        correct query in all cases.

        Parameters
        ----------
        table: expressions.Table
            Table to alter
        columns: List[expressions.ColumnDef]
            List of columns to add

        Returns
        -------
        str
            ALTER sql statement
        """
        alter_table_sql = f"ALTER TABLE {sql_to_string(table, source_type=cls.source_type)}"
        tuple_expr = expressions.Tuple(expressions=columns)
        alter_table_sql += " ADD COLUMNS " + sql_to_string(tuple_expr, source_type=cls.source_type)
        return alter_table_sql

    @classmethod
    def count_if(cls, condition: Expression) -> Expression:
        """
        Construct a COUNT_IF expression

        Parameters
        ----------
        condition: Expression
            Condition expression

        Returns
        -------
        Expression
        """
        return expressions.Anonymous(this="COUNT_IF", expressions=[condition])

    @classmethod
    def cast_to_string(cls, expr: Expression, dtype: Optional[DBVarType]) -> Expression:
        """
        Construct a CAST expression to convert the input expression to a string

        Parameters
        ----------
        expr: Expression
            Input expression
        dtype: Optional[DBVarType]
            Data type

        Returns
        -------
        Expression
        """
        _ = dtype
        return expressions.Cast(this=expr, to=expressions.DataType.build("VARCHAR"))

    def call_udf(self, udf_name: str, args: list[Expression]) -> Expression:
        """
        Construct a user defined function call expression

        Parameters
        ----------
        udf_name: str
            User defined function name
        args: list[Expression]
            List of expressions to pass as arguments to the user defined function

        Returns
        -------
        Expression
        """
        return expressions.Anonymous(this=udf_name, expressions=args)

    @classmethod
    def prepare_before_count_distinct(cls, expr: Expression, dtype: DBVarType) -> Expression:
        """
        Prepare the expression before applying COUNT_DISTINCT because some databases do not support
        COUNT_DISTINCT directly on certain data types

        Parameters
        ----------
        expr: Expression
            Expression to prepare
        dtype: DBVarType
            Data type

        Returns
        -------
        Expression
        """
        _ = dtype
        return expr

    @classmethod
    def lag_ignore_nulls(
        cls, expr: Expression, partition_by: List[Expression], order: Expression
    ) -> Expression:
        """
        Construct a LAG window function that ignores nulls

        Parameters
        ----------
        expr: Expression
            Expression to lag
        partition_by: List[Expression]
            Partition by expressions
        order: Expression
            Order expression

        Returns
        -------
        Expression
        """
        return expressions.Window(
            this=expressions.IgnoreNulls(
                this=expressions.Anonymous(this="LAG", expressions=[expr]),
            ),
            partition_by=partition_by,
            order=order,
        )

    @classmethod
    def modulo(cls, expr1: Expression, expr2: Expression) -> Expression:
        """
        Construct a modulo expression

        Parameters
        ----------
        expr1: Expression
            First expression
        expr2: Expression
            Second expression

        Returns
        -------
        Expression
        """
        return expressions.Mod(this=expr1, expression=expr2)

    @classmethod
    def normalize_timestamp_before_comparison(cls, expr: Expression) -> Expression:
        """
        Normalize the timestamp before comparison

        No op by default. This is to handle databases like BigQuery that have TIMESTAMP and DATETIME
        types that cannot be directly compared.

        Parameters
        ----------
        expr: Expression
            Expression to normalize

        Returns
        -------
        Expression
        """
        return expr

    def format_string_has_timezone(self, date_format_string: Optional[str]) -> bool:
        """
        Whether a date format string contains timezone information

        Parameters
        ----------
        date_format_string: Optional[str]
            Date format string

        Returns
        -------
        bool
        """
        if date_format_string is None:
            return False

        for pattern in self.TIMEZONE_DATE_FORMAT_EXPRESSIONS:
            if pattern in date_format_string:
                return True
        return False

    @classmethod
    def to_timestamp_from_string(cls, expr: Expression, format_string: str) -> Expression:
        """
        Convert a string to a local timestamp

        Parameters
        ----------
        expr: Expression
            Expression representing the string
        format_string: str
            Format string

        Returns
        -------
        Expression
        """
        return expressions.Anonymous(
            this="TO_TIMESTAMP", expressions=[expr, make_literal_value(format_string)]
        )

    @classmethod
    @abstractmethod
    def convert_timezone_to_utc(
        cls, expr: Expression, timezone: Expression, timezone_type: Literal["name", "offset"]
    ) -> Expression:
        """
        Convert a local timestamp to UTC timezone

        Parameters
        ----------
        expr: Expression
            Expression representing the timestamp in local timezone
        timezone: Expression
            Timezone expression
        timezone_type: Literal["name", "offset"]
            Type of timezone expression. "name" for timezone names such as "America/New_York", and
            "offset" for timezone offsets such as "-08:00"

        Returns
        -------
        Expression
        """
