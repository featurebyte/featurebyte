"""
Preview mixin for Graph Interpreter
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional
from typing import OrderedDict as OrderedDictT
from typing import Set, Tuple, cast

from collections import OrderedDict
from datetime import datetime

from sqlglot import expressions, parse_one

from featurebyte.enum import DBVarType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.metadata.operation import ViewDataColumn
from featurebyte.query_graph.sql.ast.base import ExpressionNode, TableNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import (
    MISSING_VALUE_REPLACEMENT,
    CteStatement,
    SQLType,
    construct_cte_sql,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.interpreter.base import BaseGraphInterpreter

CATEGORY_COUNT_COLUMN_NAME = "__FB_COUNTS"
CASTED_DATA_TABLE_NAME = "casted_data"
NUM_TABLES_PER_JOIN = 10


class PreviewMixin(BaseGraphInterpreter):
    """
    Preview mixin for Graph Interpreter
    """

    @staticmethod
    def _apply_type_conversions(
        sql_tree: expressions.Select, columns: List[ViewDataColumn]
    ) -> Tuple[expressions.Select, dict[Optional[str], DBVarType]]:
        """
        Apply type conversions for data retrieval

        Parameters
        ----------
        sql_tree: expressions.Select
            SQL Expression to describe
        columns: List[ViewDataColumn]
            List of columns

        Returns
        -------
        Tuple[expressions.Select, dict[Optional[str], DBVarType]]
            SQL expression for data retrieval, column to apply conversion on resulting dataframe
        """
        # convert timestamp_ntz columns to string
        type_conversions = {}
        for column in columns:
            if not column.name and len(columns) > 1:
                continue
            if column.dtype == DBVarType.TIMESTAMP_TZ:
                type_conversions[column.name] = column.dtype

        if type_conversions:
            for idx, col_expr in enumerate(sql_tree.expressions):
                col_name = col_expr.alias if col_expr.alias else col_expr.name
                type_conversion = type_conversions.get(col_name)
                if type_conversion == DBVarType.TIMESTAMP_TZ:
                    if isinstance(col_expr, expressions.Alias):
                        alias = col_expr.alias
                        col_expr = col_expr.this
                    elif col_expr.name:
                        alias = col_expr.name
                    else:
                        alias = None
                    casted_col_expr = expressions.Cast(this=col_expr, to=parse_one("STRING"))
                    if alias:
                        casted_col_expr = expressions.alias_(casted_col_expr, alias, quoted=True)
                    sql_tree.expressions[idx] = casted_col_expr

        return sql_tree, type_conversions

    def _construct_sample_sql(
        self,
        node_name: str,
        num_rows: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        timestamp_column: Optional[str] = None,
        skip_conversion: bool = False,
    ) -> Tuple[expressions.Select, dict[Optional[str], DBVarType]]:
        """Construct SQL to sample data from a given node

        Parameters
        ----------
        node_name : str
            Query graph node name
        num_rows : int
            Number of rows to sample, no sampling if None
        seed: int
            Random seed to use for sampling
        from_timestamp: Optional[datetime]
            Start of date range to filter on
        to_timestamp: Optional[datetime]
            End of date range to filter on
        timestamp_column: Optional[str]
            Column to apply date range filtering on
        skip_conversion: bool
            Whether to skip data conversion

        Returns
        -------
        Tuple[expressions.Select, dict[Optional[str], DBVarType]]
            SQL expression for data sample, column to apply conversion on resulting dataframe
        """
        flat_graph, flat_node = self.flatten_graph(node_name=node_name)
        sql_graph = SQLOperationGraph(
            flat_graph, sql_type=SQLType.MATERIALIZE, source_type=self.source_type
        )
        sql_node = sql_graph.build(flat_node)

        assert isinstance(sql_node, (TableNode, ExpressionNode))
        if isinstance(sql_node, TableNode):
            sql_tree = sql_node.sql
        else:
            sql_tree = sql_node.sql_standalone

        assert isinstance(sql_tree, expressions.Select)

        # apply type conversions
        operation_structure = QueryGraph(**self.query_graph.dict()).extract_operation_structure(
            self.query_graph.get_node_by_name(node_name), keep_all_source_columns=True
        )
        if skip_conversion:
            type_conversions: dict[Optional[str], DBVarType] = {}
        else:
            sql_tree, type_conversions = self._apply_type_conversions(
                sql_tree=sql_tree, columns=operation_structure.columns
            )

        # apply timestamp filtering
        if timestamp_column:
            filter_conditions: List[expressions.Expression] = []
            if from_timestamp:
                filter_conditions.append(
                    expressions.GTE(
                        this=quoted_identifier(timestamp_column),
                        expression=make_literal_value(
                            from_timestamp.isoformat(), cast_as_timestamp=True
                        ),
                    )
                )
            if to_timestamp:
                filter_conditions.append(
                    expressions.LT(
                        this=quoted_identifier(timestamp_column),
                        expression=make_literal_value(
                            to_timestamp.isoformat(), cast_as_timestamp=True
                        ),
                    )
                )
            if filter_conditions:
                sql_tree = sql_tree.where(expressions.and_(*filter_conditions))

        if num_rows > 0:
            # apply random sampling
            sql_tree = sql_tree.order_by(
                expressions.Anonymous(this="RANDOM", expressions=[make_literal_value(seed)])
            )
            sql_tree = sql_tree.limit(num_rows)

        return sql_tree, type_conversions

    def construct_preview_sql(
        self, node_name: str, num_rows: int = 10
    ) -> Tuple[str, dict[Optional[str], DBVarType]]:
        """Construct SQL to preview data from a given node

        Parameters
        ----------
        node_name : str
            Query graph node name
        num_rows : int
            Number of rows to include in the preview

        Returns
        -------
        Tuple[str, dict[Optional[str], DBVarType]]:
            SQL code for preview and type conversions to apply on results
        """
        sql_tree, type_conversions = self._construct_sample_sql(node_name=node_name, num_rows=0)
        return (
            sql_to_string(sql_tree.limit(num_rows), source_type=self.source_type),
            type_conversions,
        )

    def construct_sample_sql(
        self,
        node_name: str,
        num_rows: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        timestamp_column: Optional[str] = None,
    ) -> Tuple[str, dict[Optional[str], DBVarType]]:
        """Construct SQL to sample data from a given node

        Parameters
        ----------
        node_name : str
            Query graph node name
        num_rows : int
            Number of rows to include in the preview
        seed: int
            Random seed to use for sampling
        from_timestamp: Optional[datetime]
            Start of date range to filter on
        to_timestamp: Optional[datetime]
            End of date range to filter on
        timestamp_column: Optional[str]
            Column to apply date range filtering on

        Returns
        -------
        Tuple[str, dict[Optional[str], DBVarType]]:
            SQL code for sample and type conversions to apply on results
        """

        sql_tree, type_conversions = self._construct_sample_sql(
            node_name=node_name,
            num_rows=num_rows,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            timestamp_column=timestamp_column,
        )
        return sql_to_string(sql_tree, source_type=self.source_type), type_conversions

    def construct_unique_values_sql(
        self, node_name: str, column_name: str, num_rows: int = 10
    ) -> str:
        """Construct SQL to get unique values in a column from a given node

        Parameters
        ----------
        node_name : str
            Query graph node name
        column_name : str
            Column name to get unique values for
        num_rows : int
            Number of rows to include in the preview

        Returns
        -------
        str
            SQL code for getting unique column values and type conversions to apply on results
        """
        sql_tree, _ = self._construct_sample_sql(node_name=node_name, num_rows=0)
        output_expr = (
            construct_cte_sql([("data", sql_tree)])
            .select(
                expressions.Distinct(expressions=[quoted_identifier(column_name)]),
            )
            .from_("data")
        )
        return sql_to_string(output_expr.limit(num_rows), source_type=self.source_type)

    @staticmethod
    def _empty_value_expr(column_name: str) -> expressions.Expression:
        """
        Create expression for column with empty value

        Parameters
        ----------
        column_name: str
            Column name to use

        Returns
        -------
        expressions.Expression
        """
        expr = expressions.alias_(make_literal_value(None), column_name, quoted=True)
        return cast(expressions.Expression, expr)

    def _percentile_expr(
        self, expression: expressions.Expression, quantile: float
    ) -> expressions.Expression:
        """
        Create expression for percentile of column

        Parameters
        ----------
        expression: expressions.Expression
            Column expression to use for percentile expression
        quantile: float
            Quantile to use for percentile expression

        Returns
        -------
        expressions.Expression
        """
        return self.adapter.get_percentile_expr(expression, quantile)

    @staticmethod
    def _tz_offset_expr(timestamp_tz_expr: expressions.Expression) -> expressions.Expression:
        """
        Create expression for timezone offset of a timestamp_tz expr

        Parameters
        ----------
        timestamp_tz_expr: expressions.Expression
            Column expression for a timestamp with timezone offset

        Returns
        -------
        expressions.Expression
        """
        return expressions.Cast(
            this=expressions.Concat(
                expressions=[
                    make_literal_value("0"),
                    expressions.Anonymous(
                        this="SPLIT_PART",
                        expressions=[
                            timestamp_tz_expr,
                            make_literal_value("+"),
                            make_literal_value(2),
                        ],
                    ),
                ]
            ),
            to="DECIMAL",
        )

    @property
    def stats_expressions(
        self,
    ) -> OrderedDictT[
        str,
        Tuple[
            Optional[Callable[[expressions.Expression, int], expressions.Expression]],
            Optional[Set[DBVarType]],
        ],
    ]:
        """
        Ordered dictionary that defines the statistics to be computed for data description.
        For each entry:
        - the key is the name of the statistics to be computed
        - value is a tuple of:
            - optional function to generate SQL expression with the following signature:
                f(col_expr: expressions.Expression, column_idx: int) -> expressions.Expression
            - optional list of applicable DBVarType that the statistics supports. If None all types are supported.

        Returns
        -------
        OrderedDictT[
            str,
            Tuple[
                Optional[Callable[[expressions.Expression, int], expressions.Expression]],
                Optional[Set[DBVarType]],
            ],
        ]
        """
        stats_expressions: OrderedDictT[str, Tuple[Any, Optional[Set[DBVarType]]]] = OrderedDict()
        stats_expressions["unique"] = (
            lambda col_expr, _: expressions.Count(
                this=expressions.Distinct(expressions=[col_expr])
            ),
            None,
        )
        stats_expressions["%missing"] = (
            lambda col_expr, _: expressions.Mul(
                this=expressions.Paren(
                    this=expressions.Sub(
                        this=make_literal_value(1.0),
                        expression=expressions.Div(
                            this=expressions.Count(this=[col_expr]),
                            expression=expressions.Anonymous(
                                this="NULLIF",
                                expressions=[
                                    expressions.Count(this=[expressions.Star()]),
                                    make_literal_value(0),
                                ],
                            ),
                        ),
                    ),
                ),
                expression=make_literal_value(100),
            ),
            None,
        )
        stats_expressions["%empty"] = (
            lambda col_expr, column_idx: expressions.Anonymous(
                this="COUNT_IF",
                expressions=[
                    expressions.EQ(
                        this=col_expr,
                        expression=make_literal_value(""),
                    )
                ],
            ),
            {DBVarType.CHAR, DBVarType.VARCHAR},
        )
        stats_expressions["entropy"] = (None, {DBVarType.CHAR, DBVarType.VARCHAR})
        stats_expressions["top"] = (
            None,
            {
                DBVarType.BOOL,
                DBVarType.FLOAT,
                DBVarType.INT,
                DBVarType.TIMESTAMP,
                DBVarType.TIMESTAMP_TZ,
                DBVarType.CHAR,
                DBVarType.VARCHAR,
            },
        )
        stats_expressions["freq"] = (
            None,
            {
                DBVarType.BOOL,
                DBVarType.FLOAT,
                DBVarType.INT,
                DBVarType.TIMESTAMP,
                DBVarType.TIMESTAMP_TZ,
                DBVarType.CHAR,
                DBVarType.VARCHAR,
            },
        )
        stats_expressions["mean"] = (
            lambda col_expr, _: expressions.Avg(
                this=expressions.Cast(this=col_expr, to="DOUBLE"),
            ),
            {DBVarType.FLOAT, DBVarType.INT},
        )
        stats_expressions["std"] = (
            lambda col_expr, _: expressions.Stddev(
                this=expressions.Cast(this=col_expr, to="DOUBLE"),
            ),
            {DBVarType.FLOAT, DBVarType.INT},
        )
        stats_expressions["min"] = (
            lambda col_expr, _: expressions.Min(this=col_expr),
            {
                DBVarType.FLOAT,
                DBVarType.INT,
                DBVarType.TIMESTAMP,
                DBVarType.TIMESTAMP_TZ,
                DBVarType.DATE,
                DBVarType.TIME,
                DBVarType.TIMEDELTA,
            },
        )
        stats_expressions["25%"] = (
            lambda col_expr, _: self._percentile_expr(col_expr, 0.25),
            {DBVarType.FLOAT, DBVarType.INT},
        )
        stats_expressions["50%"] = (
            lambda col_expr, _: self._percentile_expr(col_expr, 0.5),
            {DBVarType.FLOAT, DBVarType.INT},
        )
        stats_expressions["75%"] = (
            lambda col_expr, _: self._percentile_expr(col_expr, 0.75),
            {DBVarType.FLOAT, DBVarType.INT},
        )
        stats_expressions["max"] = (
            lambda col_expr, _: expressions.Max(this=col_expr),
            {
                DBVarType.FLOAT,
                DBVarType.INT,
                DBVarType.TIMESTAMP,
                DBVarType.TIMESTAMP_TZ,
                DBVarType.DATE,
                DBVarType.TIME,
                DBVarType.TIMEDELTA,
            },
        )
        stats_expressions["min TZ offset"] = (
            lambda col_expr, _: expressions.Min(this=self._tz_offset_expr(col_expr)),
            {DBVarType.TIMESTAMP_TZ},
        )
        stats_expressions["max TZ offset"] = (
            lambda col_expr, _: expressions.Max(this=self._tz_offset_expr(col_expr)),
            {DBVarType.TIMESTAMP_TZ},
        )
        return stats_expressions

    @staticmethod
    def _is_dtype_supported(dtype: DBVarType, supported_dtypes: Optional[Set[DBVarType]]) -> bool:
        """
        Whether dtype is in supported dtypes

        Parameters
        ----------
        dtype: DBVarType
            DBVarType to check for support
        supported_dtypes: Optional[Set[DBVarType]]
            Set of DBVarType supported

        Returns
        -------
        bool
        """
        if not supported_dtypes:
            # If None all type are supported
            return True
        return dtype in supported_dtypes

    @staticmethod
    def _get_cat_counts(
        col_expr: expressions.Expression,
        num_categories_limit: int = 500,
    ) -> expressions.Select:
        return (
            expressions.select(
                col_expr,
                expressions.alias_(
                    expressions.Count(this=expressions.Star()),
                    alias=CATEGORY_COUNT_COLUMN_NAME,
                    quoted=True,
                ),
            )
            .from_(CASTED_DATA_TABLE_NAME)
            .group_by(col_expr)
            .order_by(
                expressions.Ordered(this=quoted_identifier(CATEGORY_COUNT_COLUMN_NAME), desc=True)
            )
            .limit(num_categories_limit)
        )

    def _construct_count_stats_sql(
        self, col_expr: expressions.Expression, column_idx: int, col_dtype: DBVarType
    ) -> expressions.Select:
        """
        Construct sql to compute count statistics for a column

        Parameters
        ----------
        col_expr: expressions.Expression
            Expression for column
        column_idx: int
            Column index
        col_dtype: DBVarType
            DBVarType of column

        Returns
        -------
        expressions.Select
        """
        cat_counts = self._get_cat_counts(col_expr)
        col_expr_filled_null = expressions.Case(
            ifs=[
                expressions.If(
                    this=expressions.Is(this=col_expr, expression=expressions.Null()),
                    true=make_literal_value(MISSING_VALUE_REPLACEMENT),
                )
            ],
            default=col_expr,
        )
        object_agg_expr = self.adapter.object_agg(
            col_expr_filled_null, quoted_identifier(CATEGORY_COUNT_COLUMN_NAME)
        )
        cat_count_dict = expressions.select(
            expressions.alias_(
                expression=object_agg_expr,
                alias="COUNT_DICT",
                quoted=True,
            )
        ).from_(expressions.Subquery(this=cat_counts, alias="cat_counts"))
        selections = []
        if self._is_dtype_supported(col_dtype, self.stats_expressions["entropy"][1]):
            # compute entropy
            selections.append(
                expressions.alias_(
                    expression=expressions.Anonymous(
                        this="F_COUNT_DICT_ENTROPY",
                        expressions=[
                            expressions.Column(
                                this=quoted_identifier("COUNT_DICT"), table="count_dict"
                            )
                        ],
                    ),
                    alias=f"entropy__{column_idx}",
                    quoted=True,
                )
            )
        if self._is_dtype_supported(col_dtype, self.stats_expressions["top"][1]):
            # compute most frequent value
            selections.append(
                expressions.alias_(
                    expression=expressions.Anonymous(
                        this="F_COUNT_DICT_MOST_FREQUENT",
                        expressions=[
                            expressions.Column(
                                this=quoted_identifier("COUNT_DICT"), table="count_dict"
                            )
                        ],
                    ),
                    alias=f"top__{column_idx}",
                    quoted=True,
                )
            )
            # compute most frequent count
            selections.append(
                expressions.alias_(
                    expression=expressions.Anonymous(
                        this="F_COUNT_DICT_MOST_FREQUENT_VALUE",
                        expressions=[
                            expressions.Column(
                                this=quoted_identifier("COUNT_DICT"), table="count_dict"
                            )
                        ],
                    ),
                    alias=f"freq__{column_idx}",
                    quoted=True,
                )
            )
        return expressions.select(*selections).from_(
            expressions.Subquery(this=cat_count_dict, alias="count_dict")
        )

    @staticmethod
    def _get_ctes_with_casted_data(
        sql_tree: expressions.Expression,
    ) -> Tuple[expressions.Select, List[CteStatement]]:
        cte_statements: List[CteStatement] = [("data", sql_tree)]

        # get subquery with columns casted to string to compute value counts
        casted_columns = []
        for col_expr in sql_tree.expressions:
            col_name = col_expr.alias_or_name
            # add casted columns
            casted_columns.append(
                expressions.alias_(
                    expressions.Cast(this=quoted_identifier(col_name), to=parse_one("STRING")),
                    col_name,
                    quoted=True,
                )
            )
        sql_tree = expressions.select(*casted_columns).from_("data")
        cte_statements.append((CASTED_DATA_TABLE_NAME, sql_tree))

        return sql_tree, cte_statements

    def _construct_stats_sql(  # pylint: disable=too-many-locals,too-many-branches
        self,
        sql_tree: expressions.Select,
        columns: List[ViewDataColumn],
        stats_names: Optional[List[str]] = None,
    ) -> Tuple[expressions.Select, List[str], List[ViewDataColumn]]:
        """
        Construct sql to retrieve statistics for an SQL view

        Parameters
        ----------
        sql_tree: expressions.Select
            SQL Expression to describe
        columns: List[ViewDataColumn]
            List of columns
        stats_names: Optional[List[str]]
            List of statistics to compute

        Returns
        -------
        Tuple[expressions.Select, List[str], List[ViewDataColumn]]
            Select expression, row indices, columns
        """
        columns_info = {
            column.name: column for column in columns if column.name or len(columns) == 1
        }
        sql_tree, cte_statements = self._get_ctes_with_casted_data(sql_tree)

        # only extract requested stats if specified
        required_stats_expressions = {}
        for stats_name, stats_values in self.stats_expressions.items():
            if stats_names is None or stats_name in stats_names:
                required_stats_expressions[stats_name] = stats_values

        stats_selections = []
        count_tables = []
        final_selections = []
        output_columns = []
        for column_idx, col_expr in enumerate(sql_tree.expressions):
            col_name = col_expr.alias or col_expr.name
            column = columns_info[col_name]
            output_columns.append(column)
            col_expr = quoted_identifier(col_name)

            # add dtype
            final_selections.append(
                expressions.alias_(
                    make_literal_value(column.dtype), f"dtype__{column_idx}", quoted=True
                )
            )

            entropy_required = "entropy" in required_stats_expressions and self._is_dtype_supported(
                column.dtype, required_stats_expressions["entropy"][1]
            )
            top_required = "top" in required_stats_expressions and self._is_dtype_supported(
                column.dtype, required_stats_expressions["top"][1]
            )
            if entropy_required or top_required:
                table_name = f"counts__{column_idx}"
                count_stats_sql = self._construct_count_stats_sql(
                    col_expr=col_expr, column_idx=column_idx, col_dtype=column.dtype
                )
                cte_statements.append((table_name, count_stats_sql))
                count_tables.append(table_name)

            # stats
            for stats_name, (stats_func, supported_dtypes) in required_stats_expressions.items():
                if stats_func:
                    if self._is_dtype_supported(column.dtype, supported_dtypes):
                        stats_selections.append(
                            expressions.alias_(
                                stats_func(col_expr, column_idx),
                                f"{stats_name}__{column_idx}",
                                quoted=True,
                            ),
                        )
                    else:
                        stats_selections.append(
                            self._empty_value_expr(f"{stats_name}__{column_idx}")
                        )

                if stats_name == "entropy":
                    stats_name = f"entropy__{column_idx}"
                    final_selections.append(
                        (expressions.Column(this=quoted_identifier(stats_name)))
                        if self._is_dtype_supported(
                            column.dtype, self.stats_expressions["entropy"][1]
                        )
                        else self._empty_value_expr(stats_name)
                    )
                elif stats_name == "top":
                    stats_name = f"top__{column_idx}"
                    final_selections.append(
                        (expressions.Column(this=quoted_identifier(stats_name)))
                        if self._is_dtype_supported(column.dtype, self.stats_expressions["top"][1])
                        else self._empty_value_expr(stats_name)
                    )
                elif stats_name == "freq":
                    stats_name = f"freq__{column_idx}"
                    final_selections.append(
                        (expressions.Column(this=quoted_identifier(stats_name)))
                        if self._is_dtype_supported(column.dtype, self.stats_expressions["freq"][1])
                        else self._empty_value_expr(stats_name)
                    )
                else:
                    final_selections.append(
                        expressions.Column(this=quoted_identifier(f"{stats_name}__{column_idx}"))
                    )

        # get statistics
        all_tables = []
        if stats_selections:
            sql_tree = expressions.select(*stats_selections).from_("data")
            cte_statements.append(("stats", sql_tree))
            all_tables.append("stats")
        all_tables.extend(count_tables)

        sql_tree = self._join_all_tables(cte_statements, all_tables).select(*final_selections)

        return sql_tree, ["dtype"] + list(required_stats_expressions.keys()), output_columns

    @staticmethod
    def _join_all_tables(
        cte_statements: List[CteStatement],
        tables: List[str],
    ) -> expressions.Select:
        if not tables:
            return expressions.select()

        # Join stats and counts tables in batches of NUM_TABLES_PER_JOIN tables, as joined_tables_0,
        # joined_tables_1, etc.
        joined_tables = []
        for join_index, i in enumerate(range(0, len(tables), NUM_TABLES_PER_JOIN)):
            cur_tables = tables[i : i + NUM_TABLES_PER_JOIN]
            sql_tree = expressions.select(expressions.Star()).from_(cur_tables[0])
            for table_name in cur_tables[1:]:
                sql_tree = sql_tree.join(expression=table_name, join_type="LEFT")
            table_alias = f"joined_tables_{join_index}"
            joined_tables.append(table_alias)
            cte_statements.append((table_alias, sql_tree))

        # Join all joined_tables_0, joined_tables_1, etc. together to form the final result
        sql_tree = construct_cte_sql(cte_statements)
        assert len(joined_tables) > 0
        sql_tree = sql_tree.from_(joined_tables[0])
        for table_name in joined_tables[1:]:
            sql_tree = sql_tree.join(expression=table_name, join_type="LEFT")

        return sql_tree

    def construct_describe_sql(
        self,
        node_name: str,
        num_rows: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        timestamp_column: Optional[str] = None,
        stats_names: Optional[List[str]] = None,
    ) -> Tuple[str, dict[Optional[str], DBVarType], List[str], List[ViewDataColumn]]:
        """Construct SQL to describe data from a given node

        Parameters
        ----------
        node_name : str
            Query graph node name
        num_rows : int
            Number of rows to include in the preview
        seed: int
            Random seed to use for sampling
        from_timestamp: Optional[datetime]
            Start of date range to filter on
        to_timestamp: Optional[datetime]
            End of date range to filter on
        timestamp_column: Optional[str]
            Column to apply date range filtering on
        stats_names: Optional[List[str]]
            List of statistics to compute. If None, compute all supported statistics.

        Returns
        -------
        Tuple[str, dict[Optional[str], DBVarType], List[str], List[ViewDataColumn]]
            SQL code, type conversions to apply on result, row indices, columns
        """
        operation_structure = QueryGraph(**self.query_graph.dict()).extract_operation_structure(
            self.query_graph.get_node_by_name(node_name), keep_all_source_columns=True
        )

        sql_tree, type_conversions = self._construct_sample_sql(
            node_name=node_name,
            num_rows=num_rows,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            timestamp_column=timestamp_column,
            skip_conversion=True,
        )

        sql_tree, row_indices, columns = self._construct_stats_sql(
            sql_tree=sql_tree, columns=operation_structure.columns, stats_names=stats_names
        )
        return (
            sql_to_string(sql_tree, source_type=self.source_type),
            type_conversions,
            row_indices,
            columns,
        )

    def construct_value_counts_sql(
        self,
        node_name: str,
        num_rows: int,
        num_categories_limit: int,
        seed: int = 1234,
    ) -> str:
        """
        Construct SQL to get value counts for a given node.

        Parameters
        ----------
        node_name : str
            Query graph node name
        num_rows : int
            Number of rows to include when calculating the counts
        num_categories_limit : int
            Maximum number of categories to include in the result. If there are more categories in
            the data, the result will include the most frequent categories up to this number.
        seed: int
            Random seed to use for sampling

        Returns
        -------
        str
        """
        sql_tree, _ = self._construct_sample_sql(
            node_name=node_name,
            num_rows=num_rows,
            seed=seed,
        )
        sql_tree, cte_statements = self._get_ctes_with_casted_data(sql_tree)
        # It's expected that this function is called on a node that is associated with a column and
        # not a frame, so here we simply take the first column.
        col_expr = sql_tree.expressions[0]
        col_name = col_expr.alias_or_name
        cat_counts = self._get_cat_counts(
            quoted_identifier(col_name), num_categories_limit=num_categories_limit
        )
        output_expr = (
            construct_cte_sql(cte_statements)
            .select(
                expressions.alias_(quoted_identifier(col_name), "key", quoted=True),
                expressions.alias_(
                    quoted_identifier(CATEGORY_COUNT_COLUMN_NAME), "count", quoted=True
                ),
            )
            .from_(cat_counts.subquery())
        )
        return sql_to_string(output_expr, source_type=self.source_type)
