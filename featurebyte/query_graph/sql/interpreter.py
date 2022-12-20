"""
This module contains the Query Graph Interpreter
"""
from __future__ import annotations

from typing import Any, List, Optional, Tuple, cast

from dataclasses import dataclass
from datetime import datetime

import numpy as np
from sqlglot import expressions, parse_one

from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupbyNode
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

    def construct_preview_sql(self, node_name: str, num_rows: int = 10) -> str:
        """Construct SQL to preview a given node

        Parameters
        ----------
        node_name : str
            Query graph node name
        num_rows : int
            Number of rows to include in the preview

        Returns
        -------
        str
            SQL code for preview purpose
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
        sql_code: str = sql_to_string(sql_tree.limit(num_rows), source_type=self.source_type)

        return sql_code

    def construct_sql(
        self,
        node_name: str,
        num_rows: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        timestamp_column: Optional[str] = None,
    ) -> expressions.Expression:
        """Construct SQL to sample a given node

        Parameters
        ----------
        node_name : str
            Query graph node name
        num_rows : int
            Number of rows to include in the preview
        seed: int
            Random seed to use for sampling
        from_timestamp: Optional[datetime]
            Start of date range to sample from
        to_timestamp: Optional[datetime]
            End of date range to sample from
        timestamp_column: Optional[str]
            Columnn to apply date range filtering on

        Returns
        -------
        expressions.Expression
            SQL expression for data sample
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

        # apply random ordering
        sql_tree = sql_tree.order_by(
            expressions.Anonymous(this="RANDOM", expressions=[make_literal_value(seed)])
        )

        # apply timestamp filtering
        filter_conditions: List[expressions.Expression] = []
        if from_timestamp:
            filter_conditions.append(
                expressions.GTE(
                    this=timestamp_column,
                    expression=make_literal_value(
                        from_timestamp.isoformat(), cast_as_timestamp=True
                    ),
                )
            )
        if to_timestamp:
            filter_conditions.append(
                expressions.LT(
                    this=timestamp_column,
                    expression=make_literal_value(to_timestamp.isoformat(), cast_as_timestamp=True),
                )
            )
        if filter_conditions:
            sql_tree = sql_tree.where(expressions.and_(*filter_conditions))

        if num_rows > 0:
            sql_tree = sql_tree.limit(num_rows)

        return sql_tree

    def construct_sample_sql(
        self,
        node_name: str,
        num_rows: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        timestamp_column: Optional[str] = None,
    ) -> str:
        """Construct SQL to sample a given node

        Parameters
        ----------
        node_name : str
            Query graph node name
        num_rows : int
            Number of rows to include in the preview
        seed: int
            Random seed to use for sampling
        from_timestamp: Optional[datetime]
            Start of date range to sample from
        to_timestamp: Optional[datetime]
            End of date range to sample from
        timestamp_column: Optional[str]
            Columnn to apply date range filtering on

        Returns
        -------
        str
            SQL code for preview purpose
        """

        sql_tree = self.construct_sql(
            node_name=node_name,
            num_rows=num_rows,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            timestamp_column=timestamp_column,
        )
        return sql_to_string(sql_tree, source_type=self.source_type)

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

    def construct_describe_sql(
        self,
        dtypes: List[Any],
        node_name: str,
        num_rows: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        timestamp_column: Optional[str] = None,
    ) -> Tuple[str, List[str], List[str]]:
        """Construct SQL to sample a given node

        Parameters
        ----------
        dtypes: List[Any]
            List of column dtypes
        node_name : str
            Query graph node name
        num_rows : int
            Number of rows to include in the preview
        seed: int
            Random seed to use for sampling
        from_timestamp: Optional[datetime]
            Start of date range to sample from
        to_timestamp: Optional[datetime]
            End of date range to sample from
        timestamp_column: Optional[str]
            Columnn to apply date range filtering on

        Returns
        -------
        Tuple[str, List[str], List[str]]
            SQL code, row names, column names
        """
        operation_structure = QueryGraph(**self.query_graph.dict()).extract_operation_structure(
            self.query_graph.get_node_by_name(node_name)
        )
        column_names = []
        for column in operation_structure.columns:
            assert column.name
            column_names.append(column.name)

        sql_tree = self.construct_sql(
            node_name=node_name,
            num_rows=num_rows,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            timestamp_column=timestamp_column,
        )

        # get modes
        selections = []
        for column_name, dtype in zip(column_names, dtypes):
            column_expr = quoted_identifier(column_name)
            selections.append(
                expressions.alias_(
                    (
                        expressions.Anonymous(this="MODE", expressions=[column_expr])
                        if not np.issubdtype(dtype, np.number)
                        else make_literal_value(None)
                    ),
                    f"mode__{column_name}",
                    quoted=True,
                )
            )

        cte_context = construct_cte_sql(
            [
                ("data", sql_tree),
                ("mode_values", expressions.select(*selections).from_("data").limit(1)),
            ]
        )

        stats_names = [
            "unique",
            "% missing",
            "top",
            "freq",
            "mean",
            "std",
            "min",
            "25%",
            "50%",
            "75%",
            "max",
        ]
        selections = []
        for column_name, dtype in zip(column_names, dtypes):
            column_expr = quoted_identifier(column_name)

            # unique, % missing
            selections.extend(
                [
                    expressions.Count(this=expressions.Distinct(expressions=[column_expr])),
                    expressions.Mul(
                        this=expressions.Paren(
                            this=expressions.Sub(
                                this=make_literal_value(1.0),
                                expression=expressions.Div(
                                    this=expressions.Count(this=[column_expr]),
                                    expression=expressions.Count(this=[make_literal_value("*")]),
                                ),
                            ),
                        ),
                        expression=make_literal_value(100),
                    ),
                ]
            )

            # top, freq
            if not np.issubdtype(dtype, np.number):
                selections.extend(
                    [
                        expressions.Anonymous(this="MODE", expressions=[column_expr]),
                        expressions.Anonymous(
                            this="COUNT_IF",
                            expressions=[
                                expressions.EQ(
                                    this=column_expr,
                                    expression=expressions.Column(
                                        this=quoted_identifier(f"mode__{column_name}"),
                                        table="mode_values",
                                    ),
                                )
                            ],
                        ),
                    ]
                )
            else:
                selections.extend(
                    [
                        self._empty_value_expr(f"top__{column_name}"),
                        self._empty_value_expr(f"freq__{column_name}"),
                    ]
                )

            # mean, std, min, 25%, 50%, 75%, max
            if np.issubdtype(dtype, np.number):
                converted_float_column = expressions.Anonymous(
                    this="TO_DOUBLE", expressions=[column_expr]
                )
                selections.extend(
                    [
                        expressions.Anonymous(this="AVG", expressions=[converted_float_column]),
                        expressions.Anonymous(this="STDDEV", expressions=[converted_float_column]),
                        expressions.Anonymous(this="MIN", expressions=[column_expr]),
                        self._percentile_expr(column_expr, 0.25),
                        self._percentile_expr(column_expr, 0.5),
                        self._percentile_expr(column_expr, 0.75),
                        expressions.Anonymous(this="MAX", expressions=[column_expr]),
                    ]
                )
            else:
                selections.extend(
                    [
                        self._empty_value_expr(f"mean__{column_name}"),
                        self._empty_value_expr(f"std__{column_name}"),
                        (
                            expressions.Anonymous(this="MIN", expressions=[column_expr])
                            if np.issubdtype(dtype, np.datetime64)
                            else self._empty_value_expr(f"min__{column_name}")
                        ),
                        self._empty_value_expr(f"25%__{column_name}"),
                        self._empty_value_expr(f"50%__{column_name}"),
                        self._empty_value_expr(f"75%__{column_name}"),
                        (
                            expressions.Anonymous(this="MAX", expressions=[column_expr])
                            if np.issubdtype(dtype, np.datetime64)
                            else self._empty_value_expr(f"max__{column_name}")
                        ),
                    ]
                )

        sql_tree = (
            cte_context.select(*selections)
            .from_("data")
            .join(expression="mode_values", join_type="LEFT")
        )

        return (
            sql_to_string(sql_tree, source_type=self.source_type),
            stats_names,
            column_names,
        )
