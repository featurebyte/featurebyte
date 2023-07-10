"""
Module for helper classes to generate engine specific SQL expressions
"""
# pylint: disable=too-many-lines,too-many-public-methods
from __future__ import annotations

from typing import Literal, Optional, cast

import re
import string
from abc import abstractmethod

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, alias_, select

from featurebyte.enum import DBVarType, InternalName, SourceType, StrEnum
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql import expression as fb_expressions
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    MISSING_VALUE_REPLACEMENT,
    get_fully_qualified_table_name,
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
        params = {cls.TABLESAMPLE_PERCENT_KEY: make_literal_value(sample_percent)}
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


class SnowflakeAdapter(BaseAdapter):
    """
    Helper class to generate Snowflake specific SQL expressions
    """

    # Snowflake does not support the PERCENT keyword. Setting the size parameter instead to
    # prevent the generated SQL to have the PERCENT keyword. Ideally, this should be handled by
    # sqlglot automatically but that is not the case yet.
    TABLESAMPLE_PERCENT_KEY = "size"

    class SnowflakeDataType(StrEnum):
        """
        Possible column types in Snowflake online store tables
        """

        FLOAT = "FLOAT"
        OBJECT = "OBJECT"
        TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
        TIMESTAMP_TZ = "TIMESTAMP_TZ"
        VARCHAR = "VARCHAR"
        VARIANT = "VARIANT"

    @classmethod
    def to_epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        return expressions.Anonymous(
            this="DATE_PART",
            expressions=[expressions.Identifier(this="EPOCH_SECOND"), timestamp_expr],
        )

    @classmethod
    def str_trim(
        cls, expr: Expression, character: Optional[str], side: Literal["left", "right", "both"]
    ) -> Expression:
        expression_class = {
            "left": fb_expressions.LTrim,
            "right": fb_expressions.RTrim,
            "both": fb_expressions.make_trim_expression,
        }[side]
        if character:
            return expression_class(this=expr, character=make_literal_value(character))  # type: ignore
        return expression_class(this=expr)  # type: ignore

    @classmethod
    def adjust_dayofweek(cls, extracted_expr: Expression) -> Expression:
        # pandas: Monday=0, Sunday=6; snowflake: Sunday=0, Saturday=6
        # to follow pandas behavior, add 6 then modulo 7 to perform left-shift
        return expressions.Mod(
            this=expressions.Paren(
                this=expressions.Add(this=extracted_expr, expression=make_literal_value(6))
            ),
            expression=make_literal_value(7),
        )

    @classmethod
    def dateadd_second(cls, quantity_expr: Expression, timestamp_expr: Expression) -> Expression:
        output_expr = expressions.Anonymous(
            this="DATEADD", expressions=["second", quantity_expr, timestamp_expr]
        )
        return output_expr

    @classmethod
    def dateadd_microsecond(
        cls, quantity_expr: Expression, timestamp_expr: Expression
    ) -> Expression:
        output_expr = expressions.Anonymous(
            this="DATEADD", expressions=["microsecond", quantity_expr, timestamp_expr]
        )
        return output_expr

    @classmethod
    def object_agg(cls, key_column: str | Expression, value_column: str | Expression) -> Expression:
        value_column = expressions.Anonymous(this="TO_VARIANT", expressions=[value_column])
        return expressions.Anonymous(this="OBJECT_AGG", expressions=[key_column, value_column])

    @classmethod
    def get_physical_type_from_dtype(cls, dtype: DBVarType) -> str:
        mapping = {
            DBVarType.INT: cls.SnowflakeDataType.FLOAT,
            DBVarType.FLOAT: cls.SnowflakeDataType.FLOAT,
            DBVarType.VARCHAR: cls.SnowflakeDataType.VARCHAR,
            DBVarType.OBJECT: cls.SnowflakeDataType.OBJECT,
            DBVarType.TIMESTAMP: cls.SnowflakeDataType.TIMESTAMP_NTZ,
            DBVarType.TIMESTAMP_TZ: cls.SnowflakeDataType.TIMESTAMP_TZ,
        }
        if dtype in mapping:
            return mapping[dtype]
        # Currently we don't expect features or tiles to be of any other types than above.
        # Otherwise, default to VARIANT since it can hold any data types
        return cls.SnowflakeDataType.VARIANT

    @classmethod
    def object_keys(cls, dictionary_expression: Expression) -> Expression:
        return expressions.Anonymous(this="OBJECT_KEYS", expressions=[dictionary_expression])

    @classmethod
    def in_array(cls, input_expression: Expression, array_expression: Expression) -> Expression:
        input_to_variant_expr = expressions.Anonymous(
            this="TO_VARIANT", expressions=[input_expression]
        )
        output_expr = expressions.Anonymous(
            this="ARRAY_CONTAINS",
            expressions=[input_to_variant_expr, array_expression],
        )
        return output_expr

    @classmethod
    def is_string_type(cls, column_expr: Expression) -> Expression:
        variant_expr = expressions.Anonymous(this="TO_VARIANT", expressions=[column_expr])
        output_expr = expressions.Anonymous(this="IS_VARCHAR", expressions=[variant_expr])
        return output_expr

    @classmethod
    def get_value_from_dictionary(
        cls, dictionary_expression: Expression, key_expression: Expression
    ) -> Expression:
        return expressions.Anonymous(
            this="GET", expressions=[dictionary_expression, key_expression]
        )

    @classmethod
    def convert_to_utc_timestamp(cls, timestamp_expr: Expression) -> Expression:
        return expressions.Anonymous(
            this="CONVERT_TIMEZONE", expressions=[make_literal_value("UTC"), timestamp_expr]
        )

    @classmethod
    def current_timestamp(cls) -> Expression:
        return expressions.Anonymous(this="SYSDATE")

    @classmethod
    def escape_quote_char(cls, query: str) -> str:
        # Snowflake sql escapes ' with ''. Use regex to make it safe to call this more than once.
        return re.sub("(?<!')'(?!')", "''", query)

    @classmethod
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
        destination_expr = get_fully_qualified_table_name(table_details.dict())
        return expressions.Create(
            this=expressions.Table(this=destination_expr),
            kind="TABLE",
            expression=select_expr,
        )

    @classmethod
    def online_store_pivot_prepare_value_column(
        cls,
        dtype: DBVarType,
    ) -> Expression:
        value_column_expr = quoted_identifier(InternalName.ONLINE_STORE_VALUE_COLUMN.value)

        # In Snowflake, we use the MAX aggregation function when pivoting the online store table
        # which doesn't support OBJECT type. Therefore, we need to convert the OBJECT type to a
        # string type.
        if dtype == DBVarType.OBJECT:
            return cast(
                Expression,
                alias_(
                    expressions.Anonymous(
                        this="TO_JSON",
                        expressions=[value_column_expr],
                    ),
                    alias=InternalName.ONLINE_STORE_VALUE_COLUMN.value,
                    quoted=True,
                ),
            )

        return value_column_expr

    @classmethod
    def online_store_pivot_finalise_value_column(
        cls,
        agg_result_name: str,
        dtype: DBVarType,
    ) -> Expression:
        # Snowflake's PIVOT surrounds the pivoted fields with single quotes (')
        agg_result_name_expr = quoted_identifier(f"'{agg_result_name}'")

        # Convert string type to OBJECT type if needed
        if dtype == DBVarType.OBJECT:
            return expressions.Anonymous(
                this="PARSE_JSON",
                expressions=[agg_result_name_expr],
            )

        return agg_result_name_expr

    @classmethod
    def online_store_pivot_aggregation_func(cls, value_column_expr: Expression) -> Expression:
        # Snowflake's PIVOT supports only a limited set of aggregation functions. Ideally we would
        # use ANY_VALUE, but since that is not supported we use MAX instead.
        return expressions.Max(this=value_column_expr)

    @classmethod
    def online_store_pivot_finalise_serving_name(cls, serving_name: str) -> Expression:
        # Snowflake's PIVOT surrounds the pivoted index column (the serving names) with double
        # quotes (") in some cases. This Alias removes them.
        if cls.will_pivoted_column_name_be_quoted(serving_name):
            return expressions.Alias(
                this=quoted_identifier(f'""{serving_name}""'),
                alias=quoted_identifier(serving_name),
            )
        return quoted_identifier(serving_name)

    @classmethod
    def will_pivoted_column_name_be_quoted(cls, column_name: str) -> bool:
        """
        If a column name is simple enough to be unquoted and when unquoted resolves to itself
        based on Snowflake's identifier resolution rules, the pivoted column name will not be
        quoted. This happens when the column name satisfies all the following conditions:

        * Start with a letter (A-Z, a-z) or an underscore (“_”).
        * Contain only letters, underscores, decimal digits (0-9), and dollar signs (“$”).
        * All the alphabetic characters are uppercase.

        See also: https://docs.snowflake.com/en/sql-reference/identifiers-syntax

        Parameters
        ----------
        column_name: str
            The column name to be checked

        Returns
        -------
        bool
        """
        starts_with_letter_or_underscore = column_name[0].isalpha() or column_name[0] == "_"
        invalid_characters = set(column_name) - set(string.ascii_letters + string.digits + "_$")
        does_not_contain_invalid_characters = len(invalid_characters) == 0
        all_alphabetic_chars_are_uppercase = column_name.isupper()
        will_not_be_quoted = (
            starts_with_letter_or_underscore
            and does_not_contain_invalid_characters
            and all_alphabetic_chars_are_uppercase
        )
        return not will_not_be_quoted

    @classmethod
    def get_percentile_expr(cls, input_expr: Expression, quantile: float) -> Expression:
        order_expr = expressions.Order(expressions=[expressions.Ordered(this=input_expr)])
        return expressions.WithinGroup(
            this=expressions.Anonymous(
                this="percentile_cont", expressions=[make_literal_value(quantile)]
            ),
            expression=order_expr,
        )


class DatabricksAdapter(BaseAdapter):
    """
    Helper class to generate Databricks specific SQL expressions
    """

    class DataType(StrEnum):
        """
        Possible column types in DataBricks
        """

        FLOAT = "DOUBLE"
        TIMESTAMP = "TIMESTAMP"
        STRING = "STRING"
        MAP = "MAP<STRING, DOUBLE>"

    @classmethod
    def object_agg(cls, key_column: str | Expression, value_column: str | Expression) -> Expression:
        return expressions.Anonymous(this="OBJECT_AGG", expressions=[key_column, value_column])

    @classmethod
    def to_epoch_seconds(cls, timestamp_expr: Expression) -> Expression:
        return expressions.Anonymous(this="UNIX_TIMESTAMP", expressions=[timestamp_expr])

    @classmethod
    def str_trim(
        cls, expr: Expression, character: Optional[str], side: Literal["left", "right", "both"]
    ) -> Expression:
        if character is None:
            character = " "
        character_literal = make_literal_value(character)

        def _make_ltrim_expr(ex: Expression) -> Expression:
            return expressions.Anonymous(this="LTRIM", expressions=[character_literal, ex])

        def _make_rtrim_expr(ex: Expression) -> Expression:
            return expressions.Anonymous(this="RTRIM", expressions=[character_literal, ex])

        if side == "left":
            out = _make_ltrim_expr(expr)
        elif side == "right":
            out = _make_rtrim_expr(expr)
        else:
            out = _make_ltrim_expr(_make_rtrim_expr(expr))
        return out

    @classmethod
    def adjust_dayofweek(cls, extracted_expr: Expression) -> Expression:
        # pandas: Monday=0, Sunday=6; databricks: Sunday=1, Saturday=7
        # Conversion formula: (databricks_dayofweek - 1 + 6) % 7
        return expressions.Mod(
            this=expressions.Paren(
                this=expressions.Add(this=extracted_expr, expression=make_literal_value(5))
            ),
            expression=make_literal_value(7),
        )

    @classmethod
    def _dateadd_by_casting_to_seconds(
        cls,
        quantity_expr: Expression,
        timestamp_expr: Expression,
        quantity_scale: Optional[float] = None,
    ) -> Expression:
        # DATEADD with a customisable unit is not supported in older versions of Spark (< 3.3). To
        # workaround that, convert to double (epoch seconds with sub-seconds preserved) to perform
        # the addition and then convert back to timestamp.
        timestamp_seconds = expressions.Cast(
            this=timestamp_expr, to=expressions.DataType.build("DOUBLE")
        )
        if quantity_scale is None:
            seconds_quantity = quantity_expr
        else:
            seconds_quantity = expressions.Div(
                this=quantity_expr, expression=make_literal_value(quantity_scale)
            )
        timestamp_seconds_added = expressions.Add(
            this=timestamp_seconds, expression=seconds_quantity
        )
        # Note: FROM_UNIXTIME doesn't work as it discards sub-seconds components even if sub-seconds
        # are included in the specified date format.
        return expressions.Cast(
            this=timestamp_seconds_added, to=expressions.DataType.build("TIMESTAMP")
        )

    @classmethod
    def dateadd_second(cls, quantity_expr: Expression, timestamp_expr: Expression) -> Expression:
        return cls._dateadd_by_casting_to_seconds(quantity_expr, timestamp_expr)

    @classmethod
    def dateadd_microsecond(
        cls, quantity_expr: Expression, timestamp_expr: Expression
    ) -> Expression:
        return cls._dateadd_by_casting_to_seconds(quantity_expr, timestamp_expr, quantity_scale=1e6)

    @classmethod
    def get_physical_type_from_dtype(cls, dtype: DBVarType) -> str:
        mapping = {
            DBVarType.INT: cls.DataType.FLOAT,
            DBVarType.FLOAT: cls.DataType.FLOAT,
            DBVarType.VARCHAR: cls.DataType.STRING,
            DBVarType.OBJECT: cls.DataType.MAP,
            DBVarType.TIMESTAMP: cls.DataType.TIMESTAMP,
        }
        if dtype in mapping:
            return mapping[dtype]
        return cls.DataType.STRING

    @classmethod
    def object_keys(cls, dictionary_expression: Expression) -> Expression:
        return expressions.Anonymous(this="map_keys", expressions=[dictionary_expression])

    @classmethod
    def in_array(cls, input_expression: Expression, array_expression: Expression) -> Expression:
        return expressions.Anonymous(
            this="array_contains", expressions=[array_expression, input_expression]
        )

    @classmethod
    def is_string_type(cls, column_expr: Expression) -> Expression:
        raise NotImplementedError()

    @classmethod
    def get_value_from_dictionary(
        cls, dictionary_expression: Expression, key_expression: Expression
    ) -> Expression:
        return expressions.Bracket(this=dictionary_expression, expressions=[key_expression])

    @classmethod
    def convert_to_utc_timestamp(cls, timestamp_expr: Expression) -> Expression:
        # timestamps do not have timezone information
        return timestamp_expr

    @classmethod
    def current_timestamp(cls) -> Expression:
        return expressions.Anonymous(this="current_timestamp")

    @classmethod
    def escape_quote_char(cls, query: str) -> str:
        # Databricks sql escapes ' with \'. Use regex to make it safe to call this more than once.
        return re.sub(r"(?<!\\)'", "\\'", query)

    @classmethod
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
        destination_expr = get_fully_qualified_table_name(table_details.dict())
        table_properties = [
            expressions.TableFormatProperty(this=expressions.Var(this="DELTA")),
            expressions.Property(
                this=expressions.Literal(this="delta.columnMapping.mode"), value="'name'"
            ),
            expressions.Property(
                this=expressions.Literal(this="delta.minReaderVersion"), value="'2'"
            ),
            expressions.Property(
                this=expressions.Literal(this="delta.minWriterVersion"), value="'5'"
            ),
        ]

        return expressions.Create(
            this=expressions.Table(this=destination_expr),
            kind="TABLE",
            expression=select_expr,
            properties=expressions.Properties(expressions=table_properties),
        )

    @classmethod
    def any_value(cls, expr: Expression) -> Expression:
        return expressions.Anonymous(this="first", expressions=[expr])

    @classmethod
    def get_percentile_expr(cls, input_expr: Expression, quantile: float) -> Expression:
        return expressions.Anonymous(
            this="percentile", expressions=[input_expr, make_literal_value(quantile)]
        )


class SparkAdapter(DatabricksAdapter):
    """
    Helper class to generate Spark specific SQL expressions

    Spark is the OSS version of Databricks, so it shares most of the same SQL syntax.
    """

    @classmethod
    def is_qualify_clause_supported(cls) -> bool:
        """
        Spark does not support the `QUALIFY` clause though DataBricks does.

        Returns
        -------
        bool
        """
        return False

    @classmethod
    def is_string_type(cls, column_expr: Expression) -> Expression:
        raise NotImplementedError()

    @classmethod
    def datediff_microsecond(
        cls, timestamp_expr_1: Expression, timestamp_expr_2: Expression
    ) -> Expression:
        def _to_microseconds(expr: Expression) -> Expression:
            return expressions.Mul(
                this=expressions.Cast(this=expr, to=expressions.DataType.build("DOUBLE")),
                expression=make_literal_value(1e6),
            )

        return expressions.Paren(
            this=expressions.Sub(
                this=_to_microseconds(timestamp_expr_2),
                expression=_to_microseconds(timestamp_expr_1),
            )
        )


def get_sql_adapter(source_type: SourceType) -> BaseAdapter:
    """
    Factory that returns an engine specific adapter given source type

    Parameters
    ----------
    source_type : SourceType
        Source type information

    Returns
    -------
    BaseAdapter
        Instance of BaseAdapter
    """
    if source_type == SourceType.DATABRICKS:
        return DatabricksAdapter()
    if source_type == SourceType.SPARK:
        return SparkAdapter()
    return SnowflakeAdapter()
