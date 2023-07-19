"""
Base class for SQL adapters
"""
from __future__ import annotations

from typing import Literal, Optional

from abc import abstractmethod

from numpy import format_float_positional
from sqlglot import expressions
from sqlglot.expressions import Expression, Select, alias_, select

from featurebyte.enum import DBVarType, InternalName
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    MISSING_VALUE_REPLACEMENT,
    get_qualified_column_identifier,
    quoted_identifier,
)

FB_QUALIFY_CONDITION_COLUMN = "__fb_qualify_condition_column"


class BaseAdapter:  # pylint: disable=too-many-public-methods
    """
    Helper class to generate engine specific SQL expressions
    """

    TABLESAMPLE_PERCENT_KEY = "percent"

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
        inner_agg_expr : expressions.Subqueryable:
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

    @classmethod
    @abstractmethod
    def get_value_from_dictionary(
        cls, dictionary_expression: Expression, key_expression: Expression
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
        params = {
            cls.TABLESAMPLE_PERCENT_KEY: expressions.Literal(
                this=format_float_positional(sample_percent, trim="-"), is_string=False
            )
        }
        tablesample_expr = expressions.TableSample(
            this=nested_select_expr.args["from"].expressions[0], **params
        )
        nested_select_expr.args["from"].set("expressions", [tablesample_expr])

        return nested_select_expr

    @classmethod
    @abstractmethod
    def create_table_as(cls, table_details: TableDetails, select_expr: Select) -> Expression:
        """
        Construct query to create a table using a select statement

        Parameters
        ----------
        table_details: TableDetails
            TableDetails of the table to be created
        select_expr: Select
            Select expression

        Returns
        -------
        Expression
        """

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
