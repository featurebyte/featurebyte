"""
This module contains the Query Graph Interpreter
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional
from typing import OrderedDict as OrderedDictT
from typing import Set, Tuple, cast

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime

from sqlglot import expressions, parse_one

from featurebyte.enum import DBVarType, SourceType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupbyNode
from featurebyte.query_graph.node.metadata.operation import ViewDataColumn
from featurebyte.query_graph.sql.ast.base import ExpressionNode, TableNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import (
    SQLType,
    construct_cte_sql,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.template import SqlExpressionTemplate
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer


@dataclass
class TileGenSql:
    """Information about a tile building SQL

    This information is required by the Tile Manager to perform tile related operations such as
    scheduling tile computation jobs.

    Parameters
    ----------
    sql : str
        Templated SQL code for building tiles
    columns : List[str]
        List of columns in the tile table after executing the SQL code
    time_modulo_frequency: int
        Offset used to determine the time for jobs scheduling. Should be smaller than frequency.
    frequency : int
        Job frequency. Needed for job scheduling.
    blind_spot : int
        Blind spot. Needed for job scheduling.
    windows : list[str | None]
        List of window sizes. Not needed for job scheduling, but can be used for other purposes such
        as determining the required tiles to build on demand during preview.
    """

    # pylint: disable=too-many-instance-attributes
    tile_table_id: str
    aggregation_id: str
    sql_template: SqlExpressionTemplate
    columns: list[str]
    entity_columns: list[str]
    serving_names: list[str]
    value_by_column: str | None
    tile_value_columns: list[str]
    tile_value_types: list[str]
    time_modulo_frequency: int
    frequency: int
    blind_spot: int
    windows: list[str | None]

    @property
    def sql(self) -> str:
        """
        Templated SQL code for building tiles

        Returns
        -------
        str
        """
        return cast(str, self.sql_template.render())


class TileSQLGenerator:
    """Generator for Tile-building SQL

    Parameters
    ----------
    query_graph : QueryGraphModel
    """

    def __init__(self, query_graph: QueryGraphModel, is_on_demand: bool, source_type: SourceType):
        self.query_graph = query_graph
        self.is_on_demand = is_on_demand
        self.source_type = source_type

    def construct_tile_gen_sql(self, starting_node: Node) -> list[TileGenSql]:
        """Construct a list of tile building SQLs for the given Query Graph

        There can be more than one tile table to build if the feature depends on more than one
        groupby operations. However, before we support complex features, there will only be one tile
        table to build.

        Parameters
        ----------
        starting_node : Node
            Starting node (typically corresponding to selected features) to search from

        Returns
        -------
        list[TileGenSql]
        """
        # Groupby operations requires building tiles (assuming the aggregation type supports tiling)
        tile_generating_nodes = {}
        for node in self.query_graph.iterate_nodes(starting_node, NodeType.GROUPBY):
            assert isinstance(node, GroupbyNode)
            tile_generating_nodes[node.name] = node

        sqls = []
        for node in tile_generating_nodes.values():
            info = self.make_one_tile_sql(node)
            sqls.append(info)

        return sqls

    def make_one_tile_sql(self, groupby_node: GroupbyNode) -> TileGenSql:
        """Construct tile building SQL for a specific groupby query graph node

        Parameters
        ----------
        groupby_node: GroupbyNode
            Groupby query graph node

        Returns
        -------
        TileGenSql
        """
        if self.is_on_demand:
            sql_type = SQLType.BUILD_TILE_ON_DEMAND
        else:
            sql_type = SQLType.BUILD_TILE
        groupby_sql_node = SQLOperationGraph(
            query_graph=self.query_graph, sql_type=sql_type, source_type=self.source_type
        ).build(groupby_node)
        sql = groupby_sql_node.sql
        tile_table_id = groupby_node.parameters.tile_id
        aggregation_id = groupby_node.parameters.aggregation_id
        entity_columns = groupby_sql_node.keys
        tile_value_columns = [spec.tile_column_name for spec in groupby_sql_node.tile_specs]
        tile_value_types = [spec.tile_column_type for spec in groupby_sql_node.tile_specs]
        assert tile_table_id is not None
        assert aggregation_id is not None
        sql_template = SqlExpressionTemplate(sql_expr=sql, source_type=self.source_type)
        info = TileGenSql(
            tile_table_id=tile_table_id,
            aggregation_id=aggregation_id,
            sql_template=sql_template,
            columns=groupby_sql_node.columns,
            entity_columns=entity_columns,
            tile_value_columns=tile_value_columns,
            tile_value_types=tile_value_types,
            time_modulo_frequency=groupby_node.parameters.time_modulo_frequency,
            frequency=groupby_node.parameters.frequency,
            blind_spot=groupby_node.parameters.blind_spot,
            windows=groupby_node.parameters.windows,
            serving_names=groupby_node.parameters.serving_names,
            value_by_column=groupby_node.parameters.value_by,
        )
        return info


class GraphInterpreter:
    """Interprets a given Query Graph and generates SQL for different purposes

    Parameters
    ----------
    query_graph : QueryGraphModel
        Query graph
    source_type : SourceType
        Data source type information
    """

    def __init__(self, query_graph: QueryGraphModel, source_type: SourceType):
        self.query_graph = query_graph
        self.source_type = source_type

    def flatten_graph(self, node_name: str) -> Tuple[QueryGraphModel, Node]:
        """
        Flatten the query graph (replace those graph node with flattened nodes)

        Parameters
        ----------
        node_name: str
            Target node name

        Returns
        -------
        Tuple[QueryGraphModel, str]
        """
        graph, node_name_map = GraphFlatteningTransformer(graph=self.query_graph).transform()
        node = graph.get_node_by_name(node_name_map[node_name])
        return graph, node

    def construct_tile_gen_sql(self, starting_node: Node, is_on_demand: bool) -> list[TileGenSql]:
        """Construct a list of tile building SQLs for the given Query Graph

        Parameters
        ----------
        starting_node : Node
            Starting node (typically corresponding to selected features) to search from
        is_on_demand : bool
            Whether the SQL is for on-demand tile building for historical features

        Returns
        -------
        List[TileGenSql]
        """
        flat_graph, flat_starting_node = self.flatten_graph(node_name=starting_node.name)
        generator = TileSQLGenerator(
            flat_graph, is_on_demand=is_on_demand, source_type=self.source_type
        )
        return generator.construct_tile_gen_sql(flat_starting_node)

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
            flat_graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=self.source_type
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
            self.query_graph.get_node_by_name(node_name)
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

    @staticmethod
    def _percentile_expr(
        expression: expressions.Expression, quantile: float
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
        order_expr = expressions.Order(expressions=[expressions.Ordered(this=expression)])
        return expressions.WithinGroup(
            this=expressions.Anonymous(
                this="percentile_cont", expressions=[parse_one(f"{quantile}")]
            ),
            expression=order_expr,
        )

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
                            expression=expressions.Count(this=[make_literal_value("*")]),
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
        cat_counts = (
            expressions.select(
                col_expr,
                expressions.alias_(
                    expressions.Count(this=make_literal_value("*")),
                    alias="COUNTS",
                    quoted=True,
                ),
            )
            .from_("casted_data")
            .group_by(col_expr)
            .order_by("COUNTS DESC")
            .limit(500)
        )
        cat_count_dict = expressions.select(
            expressions.alias_(
                expression=expressions.Anonymous(
                    this="object_agg",
                    expressions=[
                        col_expr,
                        quoted_identifier("COUNTS"),
                    ],
                ),
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

    def _construct_stats_sql(
        self, sql_tree: expressions.Select, columns: List[ViewDataColumn]
    ) -> Tuple[expressions.Select, List[str], List[ViewDataColumn]]:
        """
        Construct sql to retrieve statistics for an SQL view

        Parameters
        ----------
        sql_tree: expressions.Select
            SQL Expression to describe
        columns: List[ViewDataColumn]
            List of columns

        Returns
        -------
        Tuple[expressions.Select, List[str], List[ViewDataColumn]]
            Select expression, row indices, columns
        """
        columns_info = {
            column.name: column for column in columns if column.name or len(columns) == 1
        }
        cte_statements = [("data", sql_tree)]

        casted_columns = []
        stats_selections = []
        count_tables = []
        final_selections = []
        output_columns = []
        for column_idx, col_expr in enumerate(sql_tree.expressions):
            col_name = col_expr.alias or col_expr.name
            column = columns_info[col_name]
            output_columns.append(column)
            col_expr = quoted_identifier(col_name)

            # add casted columns
            casted_columns.append(
                expressions.alias_(
                    expressions.Cast(this=col_expr, to=parse_one("STRING")), col_name, quoted=True
                )
            )

            # add dtype
            final_selections.append(
                expressions.alias_(
                    make_literal_value(column.dtype), f"dtype__{column_idx}", quoted=True
                )
            )

            if self._is_dtype_supported(
                column.dtype, self.stats_expressions["entropy"][1]
            ) or self._is_dtype_supported(column.dtype, self.stats_expressions["top"][1]):
                table_name = f"counts__{column_idx}"
                count_stats_sql = self._construct_count_stats_sql(
                    col_expr=col_expr, column_idx=column_idx, col_dtype=column.dtype
                )
                cte_statements.append((table_name, count_stats_sql))
                count_tables.append(table_name)

            # stats
            for stats_name, (stats_func, supported_dtypes) in self.stats_expressions.items():
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
                    count_table_name = f"counts__{column_idx}"
                    final_selections.append(
                        (
                            expressions.Column(
                                this=quoted_identifier(stats_name), table=count_table_name
                            )
                        )
                        if self._is_dtype_supported(
                            column.dtype, self.stats_expressions["entropy"][1]
                        )
                        else self._empty_value_expr(stats_name)
                    )
                elif stats_name == "top":
                    stats_name = f"top__{column_idx}"
                    count_table_name = f"counts__{column_idx}"
                    final_selections.append(
                        (
                            expressions.Column(
                                this=quoted_identifier(stats_name), table=count_table_name
                            )
                        )
                        if self._is_dtype_supported(column.dtype, self.stats_expressions["top"][1])
                        else self._empty_value_expr(stats_name)
                    )
                elif stats_name == "freq":
                    stats_name = f"freq__{column_idx}"
                    count_table_name = f"counts__{column_idx}"
                    final_selections.append(
                        (
                            expressions.Column(
                                this=quoted_identifier(stats_name), table=count_table_name
                            )
                        )
                        if self._is_dtype_supported(column.dtype, self.stats_expressions["freq"][1])
                        else self._empty_value_expr(stats_name)
                    )
                else:
                    final_selections.append(
                        expressions.Column(
                            this=quoted_identifier(f"{stats_name}__{column_idx}"), table="stats"
                        )
                    )

        # get columns casted to string
        sql_tree = expressions.select(*casted_columns).from_("data")
        cte_statements.insert(1, ("casted_data", sql_tree))

        # get statistics
        sql_tree = expressions.select(*stats_selections).from_("data")
        cte_statements.append(("stats", sql_tree))

        sql_tree = construct_cte_sql(cte_statements).select(*final_selections).from_("stats")
        for table_name in count_tables:
            sql_tree = sql_tree.join(expression=table_name, join_type="LEFT")

        return sql_tree, ["dtype"] + list(self.stats_expressions.keys()), output_columns

    def construct_describe_sql(
        self,
        node_name: str,
        num_rows: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        timestamp_column: Optional[str] = None,
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

        Returns
        -------
        Tuple[str, dict[Optional[str], DBVarType], List[str], List[ViewDataColumn]]
            SQL code, type conversions to apply on result, row indices, columns
        """
        operation_structure = QueryGraph(**self.query_graph.dict()).extract_operation_structure(
            self.query_graph.get_node_by_name(node_name)
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
            sql_tree=sql_tree, columns=operation_structure.columns
        )
        return (
            sql_to_string(sql_tree, source_type=self.source_type),
            type_conversions,
            row_indices,
            columns,
        )
