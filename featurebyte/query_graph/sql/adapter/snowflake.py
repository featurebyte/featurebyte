"""
SnowflakeAdapter class for generating Snowflake specific SQL expressions
"""
from __future__ import annotations

from typing import List, Literal, Optional, cast

import re
import string

from sqlglot import expressions
from sqlglot.expressions import Expression, Identifier, Select, alias_, select

from featurebyte.enum import DBVarType, InternalName, SourceType, StrEnum
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql import expression as fb_expressions
from featurebyte.query_graph.sql.adapter.base import BaseAdapter, VectorAggColumn
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import (
    get_fully_qualified_table_name,
    get_qualified_column_identifier,
    quoted_identifier,
)


class SnowflakeAdapter(BaseAdapter):  # pylint: disable=too-many-public-methods
    """
    Helper class to generate Snowflake specific SQL expressions
    """

    source_type = SourceType.SNOWFLAKE

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
        ARRAY = "ARRAY"

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
            DBVarType.ARRAY: cls.SnowflakeDataType.ARRAY,
            DBVarType.EMBEDDING: cls.SnowflakeDataType.ARRAY,
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
        if dtype in DBVarType.json_conversion_types():
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
        if dtype in DBVarType.json_conversion_types():
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

    @classmethod
    def _get_groupby_table_alias(cls, index: int) -> str:
        return f"VECTOR_T{index}"

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
        group_by constructs a group by query for use in snowflake.

        This method differs from the default implementation, primarily to support vector aggregations, which call out
        to a snowflake UDTF. The structure of the query here changes to support this, as UDTFs essentially require a
        TABLE function call, and in turn requires us to join with that table, instead of performing a normal
        in-line aggregation like we do for other databases. Do note that if there are no vector aggregates, this
        function will behave identically to the default implementation.

        For example, a normal aggregation would look like

        SELECT
            key1,
            key2,
            SUM(value1) AS value1,
            SUM(value2) AS value2
        FROM
            table
        GROUP BY
            key1,
            key2

        But a vector aggregation using a UDTF would look like:

        SELECT
            key1,
            key2,
            AGG."VECTOR_AGG_RESULT" AS "value1"
        FROM (
            SELECT
                key1,
                key2,
            FROM
                table
        ) AS INITIAL_DATA, TABLE(
            VECTOR_AGGREGATE_SUM(INITIAL_DATA.key2) OVER (PARTITION BY INITIAL_DATA.key1)
        ) AS "AGG"

        As such, we have to push the UDTF into a subquery in order to retrieve the aggregate results from it.

        In order to handle multiple vector aggregations from UDTFs, and to ensure that we also support normal
        aggregations, we have to generate a subquery for each vector aggregation, and then join them all together.

        Parameters
        ----------
        input_expr: Select
            The input expression to group by
        select_keys: List[Expression]
            The select keys to group by
        agg_exprs: List[Expression]
            The aggregate expressions to group by
        keys: List[Expression]
            The keys to group by
        vector_aggregate_columns: Optional[List[VectorAggColumn]]
            The vector aggregate columns to group by
        quote_vector_agg_aliases: bool
            Whether to quote the vector aggregate aliases

        Returns
        -------
        Select
            The group by query
        """
        # pylint: disable=too-many-locals
        # If there are no vector aggregate expressions, we can use the standard group by.
        normal_groupby_expr = super().group_by(input_expr, select_keys, agg_exprs, keys)
        if not vector_aggregate_columns:
            return normal_groupby_expr

        # Generate vector aggregation joins
        vector_agg_select_keys = []
        for idx, vector_agg_col in enumerate(vector_aggregate_columns):
            vector_agg_select_keys.append(
                alias_(
                    get_qualified_column_identifier(
                        vector_agg_col.result_name, cls._get_groupby_table_alias(idx)
                    ),
                    alias=f"{vector_agg_col.result_name}",
                    quoted=quote_vector_agg_aliases,
                )
            )

        # Update agg_exprs select keys to use the aliases from the inner join subquery
        groupby_subquery_alias = "GROUP_BY_RESULT"
        new_groupby_exprs = []
        for agg_expr in agg_exprs:
            new_groupby_exprs.append(
                alias_(
                    get_qualified_column_identifier(
                        agg_expr.alias,
                        groupby_subquery_alias,
                        quote_column=quote_vector_agg_aliases,
                    ),
                    alias=agg_expr.alias,
                    quoted=quote_vector_agg_aliases,
                )
            )

        # Initialize the first join that has the initial select.
        table_alias = cls._get_groupby_table_alias(0)
        vector_expr = vector_aggregate_columns[0].aggr_expr.subquery(alias=table_alias)
        # Rename the TABLE that we're selecting the keys from to be the aggregated table, aliased by the subquery above.
        renamed_table_select_keys = []
        for select_key in select_keys:
            # Keep quoting with the original select key that is passed in
            should_quote_alias = quote_vector_agg_aliases
            if isinstance(select_key, Identifier):
                should_quote_alias = select_key.quoted
            renamed_table_select_keys.append(
                alias_(
                    get_qualified_column_identifier(select_key.alias_or_name, table_alias),
                    alias=select_key.alias_or_name,
                    quoted=should_quote_alias,
                )
            )
        left_expression = select(
            *renamed_table_select_keys, *new_groupby_exprs, *vector_agg_select_keys
        ).from_(vector_expr)
        # Chain the remaining joins with the remaining vector aggregates if there are more than one
        for idx, vector_agg_expr in enumerate(vector_aggregate_columns[1:]):
            right_expr = vector_agg_expr.aggr_expr.subquery(
                alias=cls._get_groupby_table_alias(idx + 1)
            )
            join_conditions = []
            for select_key in select_keys:
                join_conditions.append(
                    expressions.EQ(
                        this=get_qualified_column_identifier(
                            select_key.alias_or_name, cls._get_groupby_table_alias(idx)
                        ),
                        expression=get_qualified_column_identifier(
                            select_key.alias_or_name, cls._get_groupby_table_alias(idx + 1)
                        ),
                    )
                )
            left_expression = left_expression.join(
                right_expr,
                join_type="INNER",
                on=expressions.and_(*join_conditions),
            )

        # Join with normal aggregation groupby's if there are any.
        if agg_exprs:
            join_conditions = []
            for select_key in select_keys:
                quote_column = True
                if isinstance(select_key, Identifier):
                    quote_column = select_key.quoted
                join_conditions.append(
                    expressions.EQ(
                        this=get_qualified_column_identifier(
                            select_key.alias_or_name,
                            groupby_subquery_alias,
                            quote_column=quote_column,
                        ),
                        expression=get_qualified_column_identifier(
                            select_key.alias_or_name,
                            cls._get_groupby_table_alias(len(vector_aggregate_columns) - 1),
                        ),
                    )
                )
            left_expression = left_expression.join(
                normal_groupby_expr.subquery(alias=groupby_subquery_alias),
                join_type="INNER",
                on=expressions.and_(*join_conditions),
            )

        return left_expression

    @classmethod
    def haversine(
        cls,
        lat_expr_1: Expression,
        lon_expr_1: Expression,
        lat_expr_2: Expression,
        lon_expr_2: Expression,
    ) -> Expression:
        return expressions.Anonymous(
            this="HAVERSINE",
            expressions=[lat_expr_1, lon_expr_1, lat_expr_2, lon_expr_2],
        )
