"""
Module with logic related to feature SQL generation
"""
from __future__ import annotations

from typing import Iterable, Optional, Tuple, cast

from abc import ABC, abstractmethod

from sqlglot import expressions, select

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.query_graph.feature_common import (
    REQUEST_TABLE_NAME,
    AggregationSpec,
    FeatureSpec,
    construct_cte_sql,
)
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.interpreter import SQLOperationGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql import (
    MISSING_VALUE_REPLACEMENT,
    AliasNode,
    Project,
    SQLType,
    TableNode,
    escape_column_name,
    escape_column_names,
)

Window = int
Frequency = int
BlindSpot = int
TimeModuloFreq = int
AggSpecEntityIDs = Tuple[str, ...]
TileIndicesIdType = Tuple[Window, Frequency, BlindSpot, TimeModuloFreq, AggSpecEntityIDs]
TileIdType = str
AggregationSpecIdType = Tuple[TileIdType, Window, AggSpecEntityIDs]


class RequestTablePlan(ABC):
    """SQL generation for expanded request tables

    An expanded request table has the same number of rows as the original request table but with new
    columns added: __FB_LAST_TILE_INDEX and __FB_FIRST_TILE_INDEX. This corresponds to the first and
    last (exclusive) tile index when joining with the tile table.

    Since the required tile indices depend on feature job setting and not the specific
    aggregation method or input, an expanded table can be pre-computed (in the SQL as a common
    table) and shared with different features with the same feature job setting.

    Example:

    If the request data is as follows:
    ----------------------
    POINT_IN_TIME  CUST_ID
    ----------------------
    2022-04-01     C1
    2022-04-10     C2
    ----------------------

    Then an expanded request table would be similar to:
    -------------------------------------------------------------------
    POINT_IN_TIME  CUST_ID  __FB_FIRST_TILE_INDEX  __FB_LAST_TILE_INDEX
    -------------------------------------------------------------------
    2022-04-01     C1       1000                   1010
    2022-04-10     C2       1105                   1115
    -------------------------------------------------------------------
    """

    def __init__(self) -> None:
        self.expanded_request_table_names: dict[TileIndicesIdType, str] = {}

    def add_aggregation_spec(self, agg_spec: AggregationSpec) -> None:
        """Process a new AggregationSpec

        Depending on the feature job setting of the provided aggregation, a new expanded request
        table may or may not be required.

        Parameters
        ----------
        agg_spec : AggregationSpec
            Aggregation specification
        """
        unique_tile_indices_id = self.get_unique_tile_indices_id(agg_spec)
        if unique_tile_indices_id not in self.expanded_request_table_names:
            output_table_name = (
                f"REQUEST_TABLE"
                f"_W{agg_spec.window}"
                f"_F{agg_spec.frequency}"
                f"_BS{agg_spec.blind_spot}"
                f"_M{agg_spec.time_modulo_frequency}"
                f"_{'_'.join(agg_spec.serving_names)}"
            )
            self.expanded_request_table_names[unique_tile_indices_id] = output_table_name

    def get_expanded_request_table_name(self, agg_spec: AggregationSpec) -> str:
        """Get the name of the expanded request table given and AggregationSpec

        Parameters
        ----------
        agg_spec : AggregationSpec
            Aggregation specification

        Returns
        -------
        str
            Expanded request table name
        """
        key = self.get_unique_tile_indices_id(agg_spec)
        return self.expanded_request_table_names[key]

    @staticmethod
    def get_unique_tile_indices_id(agg_spec: AggregationSpec) -> TileIndicesIdType:
        """Get a key for an AggregationSpec that controls reuse of expanded request table

        Parameters
        ----------
        agg_spec : AggregationSpec
            Aggregation specification

        Returns
        -------
        tuple
        """
        unique_tile_indices_id = (
            agg_spec.window,
            agg_spec.frequency,
            agg_spec.blind_spot,
            agg_spec.time_modulo_frequency,
            tuple(agg_spec.serving_names),
        )
        return unique_tile_indices_id

    def construct_request_tile_indices_ctes(self) -> list[tuple[str, str]]:
        """Construct SQL statements that build the expanded request tables

        Returns
        -------
        list[tuple[str, str]]
        """
        expanded_request_ctes = []
        for unique_tile_indices_id, table_name in self.expanded_request_table_names.items():
            (
                window_size,
                frequency,
                blind_spot,
                time_modulo_frequency,
                serving_names,
            ) = unique_tile_indices_id
            expanded_table_sql = self.construct_expanded_request_table_sql(
                window_size=window_size,
                frequency=frequency,
                blind_spot=blind_spot,
                time_modulo_frequency=time_modulo_frequency,
                serving_names=list(serving_names),
            )
            expanded_request_ctes.append((escape_column_name(table_name), expanded_table_sql))
        return expanded_request_ctes

    @staticmethod
    @abstractmethod
    def construct_expanded_request_table_sql(
        window_size: int,
        frequency: int,
        blind_spot: int,
        time_modulo_frequency: int,
        serving_names: list[str],
    ) -> str:
        """Construct SQL for expanded SQLs

        The query can be different for different data warehouses.

        Parameters
        ----------
        window_size : int
            Feature window size
        frequency : int
            Frequency in feature job setting
        time_modulo_frequency : int
            Time modulo frequency in feature job setting
        blind_spot : int
            Blind spot in feature job setting
        serving_names: list[str]
            List of serving names corresponding to entities

        Returns
        -------
        str
            SQL code for expanding request table
        """


class SnowflakeRequestTablePlan(RequestTablePlan):
    """Generator of Snowflake specific query to expand request table"""

    @staticmethod
    def construct_expanded_request_table_sql(
        window_size: int,
        frequency: int,
        blind_spot: int,
        time_modulo_frequency: int,
        serving_names: list[str],
    ) -> str:
        # Input request table can have duplicated time points but aggregation should be done only on
        # distinct time points
        quoted_serving_names = escape_column_names(serving_names)
        select_distinct_expr = (
            expressions.Select(distinct=True)
            .select(SpecialColumnName.POINT_IN_TIME.value, *quoted_serving_names)
            .from_(REQUEST_TABLE_NAME)
        )
        num_tiles = window_size // frequency
        expr = select(
            SpecialColumnName.POINT_IN_TIME.value,
            *quoted_serving_names,
            f"DATE_PART(epoch, {SpecialColumnName.POINT_IN_TIME}) AS __FB_TS",
            f"FLOOR((__FB_TS - {time_modulo_frequency}) / {frequency}) AS {InternalName.LAST_TILE_INDEX}",
            f"{InternalName.LAST_TILE_INDEX} - {num_tiles} AS {InternalName.FIRST_TILE_INDEX}",
        ).from_(select_distinct_expr.subquery())
        expr_str = cast(str, expr.sql(pretty=True))
        return expr_str


class AggregationSpecSet:
    """Responsible for keeping track of AggregationSpec that arises from query graph nodes"""

    def __init__(self) -> None:
        self.aggregation_specs: dict[AggregationSpecIdType, list[AggregationSpec]] = {}
        self.processed_agg_specs: dict[AggregationSpecIdType, set[str]] = {}

    def add_aggregation_spec(self, aggregation_spec: AggregationSpec) -> None:
        """Update state given an AggregationSpec

        Some aggregations can be shared by different features, e.g. "transaction_type (7 day
        entropy)" and "transaction_type (7 day most frequent)" can both reuse the aggregated result
        of "transaction (7 day category count by transaction_type)". This information is tracked
        using the aggregation_id attribute of AggregationSpec - the AggregationSpec for all of these
        three features will have the same aggregation_id.

        Parameters
        ----------
        aggregation_spec : AggregationSpec
            Aggregation_specification
        """
        # AggregationSpec is window size specific. Two AggregationSpec with different window sizes
        # require two different groupbys and left joins in the resulting SQL. Include serving_names
        # here because each group of AggregationSpecs will be joined with exactly one expanded
        # request table, and an expanded request table is specific to serving names
        key = (
            aggregation_spec.tile_table_id,
            aggregation_spec.window,
            tuple(aggregation_spec.serving_names),
        )

        # Initialize containers for new tile_table_id and window combination
        if key not in self.aggregation_specs:
            self.aggregation_specs[key] = []
            self.processed_agg_specs[key] = set()

        agg_id = aggregation_spec.aggregation_id
        # Skip if the same AggregationSpec is already seen
        if agg_id in self.processed_agg_specs[key]:
            return

        # Update containers
        self.aggregation_specs[key].append(aggregation_spec)
        self.processed_agg_specs[key].add(agg_id)

    def get_grouped_aggregation_specs(self) -> Iterable[list[AggregationSpec]]:
        """Yields groups of AggregationSpec

        Each group of AggregationSpec has the same tile_table_id. Their tile values can be
        aggregated in a single GROUP BY clause.

        Yields
        ------
        list[AggregationSpec]
            Group of AggregationSpec
        """
        for agg_specs in self.aggregation_specs.values():
            yield agg_specs


class FeatureExecutionPlan(ABC):
    """Responsible for constructing the SQL to compute features by aggregating tiles"""

    AGGREGATION_TABLE_NAME = "_FB_AGGREGATED"

    def __init__(self) -> None:
        self.aggregation_spec_set = AggregationSpecSet()
        self.feature_specs: dict[str, FeatureSpec] = {}
        self.request_table_plan: RequestTablePlan = SnowflakeRequestTablePlan()

    @property
    def required_serving_names(self) -> set[str]:
        """Returns the list of required serving names

        Returns
        -------
        set[str]
        """
        out = set()
        for agg_specs in self.aggregation_spec_set.get_grouped_aggregation_specs():
            for agg_spec in agg_specs:
                out.update(agg_spec.serving_names)
        return out

    def add_aggregation_spec(self, aggregation_spec: AggregationSpec) -> None:
        """Add AggregationSpec to be incorporated when generating SQL

        Parameters
        ----------
        aggregation_spec : AggregationSpec
            Aggregation specification
        """
        self.aggregation_spec_set.add_aggregation_spec(aggregation_spec)
        self.request_table_plan.add_aggregation_spec(aggregation_spec)

    def add_feature_spec(self, feature_spec: FeatureSpec) -> None:
        """Add FeatureSpec to be incorporated when generating SQL

        Parameters
        ----------
        feature_spec : FeatureSpec
            Feature specification

        Raises
        ------
        ValueError
            If there are duplicated feature names
        """
        key = feature_spec.feature_name
        if key in self.feature_specs:
            raise ValueError(f"Duplicated feature name: {key}")
        self.feature_specs[key] = feature_spec

    @classmethod
    def construct_aggregation_sql(
        cls,
        expanded_request_table_name: str,
        tile_table_id: str,
        point_in_time_column: str,
        keys: list[str],
        serving_names: list[str],
        value_by: str | None,
        merge_exprs: list[str],
        agg_result_names: list[str],
        num_tiles: int,
    ) -> expressions.Select:
        """Construct SQL code for one specific aggregation

        The aggregation consists of inner joining with the tile table on entity id and required tile
        indices and applying the merge expression.

        When value_by is set, the aggregation above produces an intermediate result that look
        similar to below since tiles building takes into account the category:

        --------------------------------------
        POINT_IN_TIME  ENTITY  CATEGORY  VALUE
        --------------------------------------
        2022-01-01     C1      K1        1
        2022-01-01     C1      K2        2
        2022-01-01     C2      K3        3
        2022-01-01     C3      K1        4
        ...
        --------------------------------------

        We can aggregate the above into key-value pairs by aggregating over point-in-time and entity
        and applying functions such as OBJECT_AGG:

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
        expanded_request_table_name : str
            Expanded request table name
        tile_table_id: str
            Tile table name
        point_in_time_column : str
            Point in time column name
        keys : list[str]
            List of join key columns
        serving_names : list[str]
            List of serving name columns
        value_by : str | None
            Optional category parameter for the groupby operation
        merge_exprs : list[str]
            SQL expressions that aggregates intermediate values stored in tile table
        agg_result_names : list[str]
            Column names of the aggregated results
        num_tiles : int
            Feature window size in terms of number of tiles (function of frequency)

        Returns
        -------
        expressions.Select
        """
        # pylint: disable=too-many-locals
        last_index_name = InternalName.LAST_TILE_INDEX.value
        range_join_condition = expressions.or_(
            f"FLOOR(REQ.{last_index_name} / {num_tiles}) = FLOOR(TILE.INDEX / {num_tiles})",
            f"FLOOR(REQ.{last_index_name} / {num_tiles}) - 1 = FLOOR(TILE.INDEX / {num_tiles})",
        )
        join_conditions_lst = [range_join_condition]
        for serving_name, key in zip(escape_column_names(serving_names), escape_column_names(keys)):
            join_conditions_lst.append(f"REQ.{serving_name} = TILE.{key}")
        join_conditions = expressions.and_(*join_conditions_lst)

        first_index_name = InternalName.FIRST_TILE_INDEX.value
        range_join_where_conditions = [
            f"TILE.INDEX >= REQ.{first_index_name}",
            f"TILE.INDEX < REQ.{last_index_name}",
        ]

        group_by_keys = [f"REQ.{point_in_time_column}"]
        for serving_name in escape_column_names(serving_names):
            group_by_keys.append(f"REQ.{serving_name}")

        if value_by is None:
            inner_agg_result_names = agg_result_names
            inner_group_by_keys = group_by_keys
        else:
            inner_agg_result_names = [
                f"inner_{agg_result_name}" for agg_result_name in agg_result_names
            ]
            inner_group_by_keys = group_by_keys + [f"TILE.{escape_column_name(value_by)}"]

        inner_agg_expr = (
            select(
                *inner_group_by_keys,
                *[
                    f'{merge_expr} AS "{inner_agg_result_name}"'
                    for merge_expr, inner_agg_result_name in zip(
                        merge_exprs, inner_agg_result_names
                    )
                ],
            )
            .from_(f"{escape_column_name(expanded_request_table_name)} AS REQ")
            .join(
                tile_table_id,
                join_alias="TILE",
                join_type="inner",
                on=join_conditions,
            )
            .where(*range_join_where_conditions)
            .group_by(*inner_group_by_keys)
        )

        if value_by is None:
            agg_expr = inner_agg_expr
        else:
            agg_expr = cls.construct_key_value_aggregation_sql(
                point_in_time_column=point_in_time_column,
                serving_names=serving_names,
                value_by=value_by,
                agg_result_names=agg_result_names,
                inner_agg_result_names=inner_agg_result_names,
                inner_agg_expr=inner_agg_expr,
            )

        return agg_expr

    @classmethod
    @abstractmethod
    def construct_key_value_aggregation_sql(
        cls,
        point_in_time_column: str,
        serving_names: list[str],
        value_by: str,
        agg_result_names: list[str],
        inner_agg_result_names: list[str],
        inner_agg_expr: expressions.Select,
    ) -> expressions.Select:
        """Aggregate per category values into key value pairs

        # noqa: DAR103

        Parameters
        ----------
        point_in_time_column : str
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

    @staticmethod
    def construct_left_join_sql(
        index: int,
        point_in_time_column: str,
        agg_result_names: list[str],
        serving_names: list[str],
        table_expr: expressions.Select,
        agg_expr: expressions.Select,
    ) -> tuple[expressions.Select, list[str]]:
        """Construct SQL that left join aggregated result back to request table

        Parameters
        ----------
        index : int
            Index of the current left join
        point_in_time_column : str
            Point in time column
        agg_result_names : list[str]
            Column names of the aggregated results
        serving_names : list[str]
            List of serving name columns
        table_expr : expressions.Select
            Table to which the left join should be added to
        agg_expr : expressions.Select
            SQL expression that performs the aggregation

        Returns
        -------
        tuple[Select, str]
            Tuple of updated table expression and alias name for the aggregated column
        """
        agg_table_alias = f"T{index}"
        agg_result_name_aliases = [
            f'"{agg_table_alias}"."{agg_result_name}" AS "{agg_result_name}"'
            for agg_result_name in agg_result_names
        ]
        join_conditions_lst = [
            f"REQ.{point_in_time_column} = {agg_table_alias}.{point_in_time_column}",
        ]
        for serving_name in serving_names:
            join_conditions_lst += [
                f"REQ.{escape_column_name(serving_name)} = {agg_table_alias}.{escape_column_name(serving_name)}"
            ]
        updated_table_expr = table_expr.join(
            agg_expr.subquery(),
            join_type="left",
            join_alias=agg_table_alias,
            on=expressions.and_(*join_conditions_lst),
        )
        return updated_table_expr, agg_result_name_aliases

    def construct_combined_aggregation_cte(
        self, point_in_time_column: str, request_table_columns: list[str]
    ) -> tuple[str, str]:
        """Construct SQL code for all aggregations

        Parameters
        ----------
        point_in_time_column : str
            Point in time column
        request_table_columns : list[str]
            Request table columns

        Returns
        -------
        tuple[str, str]
            Tuple of table name and SQL code
        """
        table_expr = select().from_(f"{REQUEST_TABLE_NAME} AS REQ")
        qualified_aggregation_names = []

        for i, agg_specs in enumerate(self.aggregation_spec_set.get_grouped_aggregation_specs()):
            # All AggregationSpec in agg_specs share common attributes such as tile_table_id, keys,
            # etc. Get the first one to access them.
            agg_spec = agg_specs[0]
            expanded_request_table_name = self.request_table_plan.get_expanded_request_table_name(
                agg_spec
            )
            merge_exprs = [agg_spec.merge_expr for agg_spec in agg_specs]
            agg_result_names = [agg_spec.agg_result_name for agg_spec in agg_specs]
            agg_expr = self.construct_aggregation_sql(
                expanded_request_table_name=expanded_request_table_name,
                tile_table_id=agg_spec.tile_table_id,
                point_in_time_column=point_in_time_column,
                keys=agg_spec.keys,
                serving_names=agg_spec.serving_names,
                value_by=agg_spec.value_by,
                merge_exprs=merge_exprs,
                agg_result_names=agg_result_names,
                num_tiles=agg_spec.window // agg_spec.frequency,
            )
            table_expr, agg_result_name_aliases = self.construct_left_join_sql(
                index=i,
                point_in_time_column=point_in_time_column,
                agg_result_names=agg_result_names,
                serving_names=agg_spec.serving_names,
                table_expr=table_expr,
                agg_expr=agg_expr,
            )
            qualified_aggregation_names.extend(agg_result_name_aliases)

        request_table_columns = [f"REQ.{escape_column_name(c)}" for c in request_table_columns]
        table_expr = table_expr.select(*request_table_columns, *qualified_aggregation_names)
        combined_sql = table_expr.sql(pretty=True)

        return self.AGGREGATION_TABLE_NAME, combined_sql

    def construct_post_aggregation_sql(self, request_table_columns: list[str]) -> str:
        """Construct SQL code for post-aggregation that transforms aggregated results to features

        Most of the time aggregated results are the features. However, some features require
        additional transforms (e.g. UDF, arithmetic expressions, fillna, etc) after aggregation.

        Columns in the request table is required so that all columns in the request table can be
        passed through.

        Parameters
        ----------
        request_table_columns : list[str]
            Columns in the input request table

        Returns
        -------
        str
        """
        qualified_feature_names = []
        for feature_spec in self.feature_specs.values():
            feature_alias = (
                f"{feature_spec.feature_expr} AS {escape_column_name(feature_spec.feature_name)}"
            )
            qualified_feature_names.append(feature_alias)
        request_table_column_names = [
            f"AGG.{escape_column_name(col)}" for col in request_table_columns
        ]
        table_expr = select(*request_table_column_names, *qualified_feature_names).from_(
            f"{self.AGGREGATION_TABLE_NAME} AS AGG"
        )
        sql = cast(str, table_expr.sql(pretty=True))
        return sql

    def construct_combined_sql(
        self,
        point_in_time_column: str,
        request_table_columns: list[str],
        prior_cte_statements: Optional[list[tuple[str, str]]] = None,
    ) -> str:
        """Construct combined SQL that will generate the features

        Parameters
        ----------
        point_in_time_column : str
            Point in time column
        request_table_columns : list[str]
            Request table columns
        prior_cte_statements : Optional[list[tuple[str, str]]]
            Other CTE statements to incorporate to the final SQL (namely the request data SQL and
            on-demand tile SQL)

        Returns
        -------
        str
        """
        cte_statements = []
        if prior_cte_statements is not None:
            assert isinstance(prior_cte_statements, list)
            cte_statements.extend(prior_cte_statements)

        cte_statements.extend(self.request_table_plan.construct_request_tile_indices_ctes())
        cte_statements.append(
            self.construct_combined_aggregation_cte(point_in_time_column, request_table_columns)
        )
        cte_sql = construct_cte_sql(cte_statements)

        post_aggregation_sql = self.construct_post_aggregation_sql(request_table_columns)
        sql = "\n".join([cte_sql, post_aggregation_sql])
        return sql


class SnowflakeFeatureExecutionPlan(FeatureExecutionPlan):
    """Snowflake specific implementation of FeatureExecutionPlan"""

    @classmethod
    def construct_key_value_aggregation_sql(
        cls,
        point_in_time_column: str,
        serving_names: list[str],
        value_by: str,
        agg_result_names: list[str],
        inner_agg_result_names: list[str],
        inner_agg_expr: expressions.Select,
    ) -> expressions.Select:

        inner_alias = "INNER_"

        outer_group_by_keys = [f"{inner_alias}.{point_in_time_column}"]
        for serving_name in serving_names:
            outer_group_by_keys.append(f"{inner_alias}.{escape_column_name(serving_name)}")

        category_col = f"{inner_alias}.{escape_column_name(value_by)}"
        # Cast type to string first so that integer can be represented nicely ('{"0": 7}' vs
        # '{"0.00000": 7}')
        category_col_casted = f"CAST({category_col} AS VARCHAR)"
        # Replace missing category values since OBJECT_AGG ignores keys that are null
        category_filled_null = (
            f"CASE WHEN {category_col} IS NULL THEN '{MISSING_VALUE_REPLACEMENT}' ELSE "
            f"{category_col_casted} END"
        )
        object_agg_exprs = [
            f'OBJECT_AGG({category_filled_null}, {inner_alias}."{inner_agg_result_name}")'
            f' AS "{agg_result_name}"'
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


class FeatureExecutionPlanner:
    """Responsible for constructing a FeatureExecutionPlan given QueryGraph and Node

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    """

    def __init__(self, graph: QueryGraph, serving_names_mapping: dict[str, str] | None = None):
        self.graph = graph
        self.plan = SnowflakeFeatureExecutionPlan()
        self.serving_names_mapping = serving_names_mapping

    def generate_plan(self, nodes: list[Node]) -> FeatureExecutionPlan:
        """Generate FeatureExecutionPlan for given query graph Node

        Parameters
        ----------
        nodes : list[Node]
            Query graph nodes

        Returns
        -------
        FeatureExecutionPlan
        """
        for node in nodes:
            self.process_node(node)
        return self.plan

    def process_node(self, node: Node) -> None:
        """Generate FeatureExecutionPlan for given query graph Node

        Parameters
        ----------
        node : Node
            Query graph node
        """
        for groupby_node in self.graph.iterate_grouby_nodes(node):
            self.parse_and_update_specs_from_groupby(groupby_node)
        self.update_feature_specs(node)

    def parse_and_update_specs_from_groupby(self, groupby_node: Node) -> None:
        """Update FeatureExecutionPlan with a groupby query node

        Parameters
        ----------
        groupby_node : Node
            Groupby query node
        """
        agg_specs = AggregationSpec.from_groupby_query_node(
            groupby_node, serving_names_mapping=self.serving_names_mapping
        )
        for agg_spec in agg_specs:
            self.plan.add_aggregation_spec(agg_spec)

    def update_feature_specs(self, node: Node) -> None:
        """Update FeatureExecutionPlan with a query graph node

        Parameters
        ----------
        node : Node
            Query graph node
        """
        sql_graph = SQLOperationGraph(self.graph, SQLType.GENERATE_FEATURE)
        sql_node = sql_graph.build(node)

        if isinstance(sql_node, TableNode):
            for feature_name, feature_expr in sql_node.columns_map.items():
                feature_spec = FeatureSpec(
                    feature_name=feature_name,
                    feature_expr=feature_expr.sql(),
                )
                self.plan.add_feature_spec(feature_spec)
        else:
            if isinstance(sql_node, Project):
                feature_name = sql_node.column_name
            elif isinstance(sql_node, AliasNode):
                feature_name = sql_node.name
            else:
                # Otherwise, there is no way to know about the feature name. Technically speaking
                # this could still be previewed as an "unnamed" feature since the expression is
                # available, but it cannot be published.
                feature_name = "Unnamed"
            feature_expr = sql_node.sql.sql()
            feature_spec = FeatureSpec(feature_name=feature_name, feature_expr=feature_expr)
            self.plan.add_feature_spec(feature_spec)
