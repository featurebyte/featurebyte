"""
Module with logic related to feature SQL generation
"""
from __future__ import annotations

from typing import Any, Optional

import logging
import time

import pandas as pd
import sqlglot
from sqlglot import select

from featurebyte.query_graph.feature_sql import AggregationSpec, FeatureSpec
from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.query_graph.interpreter import SQLOperationGraph, find_parent_groupby_nodes
from featurebyte.query_graph.sql import Project, SQLType, TableNode
from featurebyte.query_graph.tile_compute import construct_on_demand_tile_ctes

REQUEST_TABLE_NAME = "REQUEST_TABLE"

logger = logging.getLogger("featurebyte")


def prettify_sql(sql_str: str) -> str:
    """Reformat sql code using sqlglot

    Parameters
    ----------
    sql_str : str
        SQL code to be prettified

    Returns
    -------
    str
    """
    result = sqlglot.parse_one(sql_str).sql(pretty=True)
    assert isinstance(result, str)
    return result


def construct_preview_request_table_sql(
    request_dataframe: pd.DataFrame, date_cols: list[str]
) -> str:
    """Construct a SELECT statement that uploads the request data

    This does not use write_pandas and should only be used for small request data (e.g. request data
    during preview that has only one row)

    Parameters
    ----------
    request_dataframe : DataFrame
        Request dataframe
    date_cols : list[str]
        List of date columns

    Returns
    -------
    str
    """
    row_sqls = []
    for _, row in request_dataframe.iterrows():
        columns = []
        for col, value in row.items():
            if col in date_cols:
                expr = f"CAST('{str(value)}' AS TIMESTAMP)"
            else:
                if isinstance(value, str):
                    expr = f"'{value}'"
                else:
                    expr = value
            columns.append(f"{expr} AS {col}")
        row_sql = select(*columns).sql()
        row_sqls.append(row_sql)
    return prettify_sql(" UNION ALL\n".join(row_sqls))


class RequestTablePlan:
    """SQL generation for expanded request tables

    An expanded request table contains a tile index column (REQ_TILE_INDEX) representing the
    required tiles for a windowed aggregation. It will be used as a join key when joining with the
    tile table. Since the required tile indices depend on feature job setting and not the specific
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
    --------------------------------------
    POINT_IN_TIME  CUST_ID  REQ_TILE_INDEX
    --------------------------------------
    2022-04-01     C1       2500000
    2022-04-01     C1       2500001
    2022-04-01     C1       2500002
    2022-04-01     C1       2500003
    2022-04-01     C1       2500004
    2022-04-10     C2       2500010
    2022-04-10     C2       2500011
    2022-04-10     C2       2500012
    2022-04-10     C2       2500013
    2022-04-10     C2       2500014
    --------------------------------------

    The REQ_TILE_INDEX column will be used as a join key when joining with the tile table.
    """

    TileIndicesIdType = tuple[int, int, int, int, tuple[str, ...]]  # type: ignore[misc]

    def __init__(self) -> None:
        self.expanded_request_table_names: dict[RequestTablePlan.TileIndicesIdType, str] = {}

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
                f"_{'_'.join(agg_spec.entity_ids)}"
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
            tuple(agg_spec.entity_ids),
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
                entity_columns,
            ) = unique_tile_indices_id
            expanded_table_sql = construct_expanded_request_table_sql(
                window_size=window_size,
                frequency=frequency,
                blind_spot=blind_spot,
                time_modulo_frequency=time_modulo_frequency,
                entity_columns=list(entity_columns),
            )
            expanded_request_ctes.append((table_name, expanded_table_sql))
        return expanded_request_ctes


class FeatureExecutionPlan:
    """Responsible for constructing the SQL to compute features by aggregating tiles"""

    AGGREGATION_TABLE_NAME = "_FB_AGGREGATED"

    def __init__(self) -> None:
        self.aggregation_specs: dict[tuple[str, int], AggregationSpec] = {}
        self.feature_specs: dict[str, FeatureSpec] = {}
        self.request_table_plan: RequestTablePlan = RequestTablePlan()

    def add_aggregation_spec(self, aggregation_spec: AggregationSpec) -> None:
        """Add AggregationSpec to be incorporated when generating SQL

        Parameters
        ----------
        aggregation_spec : AggregationSpec
            Aggregation specification
        """
        key = self.get_aggregation_spec_key(aggregation_spec)
        self.aggregation_specs[key] = aggregation_spec
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

    @staticmethod
    def get_aggregation_spec_key(aggregation_spec: AggregationSpec) -> tuple[str, int]:
        """Get a key for a AggregationSpec that determines whether it can be shared

        Some aggregations can be shared by different features, e.g. "transaction_type (7 day
        entropy)" and "transaction_type (7 day most frequent)" can both reuse the aggregated result
        of "transaction (7 day category count by transaction_type)".

        Note that this is different from tile table reuse. Tile table reuse depends on
        tile_table_id and does not consider feature window size.

        Parameters
        ----------
        aggregation_spec : AggregationSpec
            Aggregation_specification

        Returns
        -------
        tuple
        """
        tile_table_id = aggregation_spec.tile_table_id
        window = aggregation_spec.window
        return tile_table_id, window

    @staticmethod
    def construct_aggregation_sql(
        expanded_request_table_name: str,
        tile_table_id: str,
        point_in_time_column: str,
        entity_ids: list[str],
        merge_expr: str,
        agg_result_name: str,
    ) -> str:
        """Construct SQL code for one specific aggregation

        Parameters
        ----------
        expanded_request_table_name : str
            Expanded request table name
        tile_table_id: str
            Tile table name
        point_in_time_column : str
            Point in time column name
        entity_ids : list[str]
            List of entity IDs
        merge_expr : str
            SQL expression that aggregates intermediate values stored in tile table
        agg_result_name : str
            Column name of the aggregated result

        Returns
        -------
        str
        """
        join_conditions_lst = ["REQ.REQ_TILE_INDEX = TILE.INDEX"]
        for key in entity_ids:
            join_conditions_lst.append(f"REQ.{key} = TILE.{key}")
        join_conditions = " AND ".join(join_conditions_lst)

        group_by_keys_lst = [f"REQ.{point_in_time_column}"]
        for key in entity_ids:
            group_by_keys_lst.append(f"REQ.{key}")
        group_by_keys = ", ".join(group_by_keys_lst)

        sql = f"""
            SELECT
                {group_by_keys},
                {merge_expr} AS "{agg_result_name}"
            FROM {expanded_request_table_name} REQ
            INNER JOIN {tile_table_id} TILE
            ON {join_conditions}
            GROUP BY {group_by_keys}
            """

        return prettify_sql(sql)

    def construct_combined_aggregation_cte(self, point_in_time_column: str) -> tuple[str, str]:
        """Construct SQL code for all aggregations

        Parameters
        ----------
        point_in_time_column : str
            Point in time column

        Returns
        -------
        tuple[str, str]
            Tuple of table name and SQL code
        """
        left_joins = []
        qualified_aggregation_names = []
        for i, agg_spec in enumerate(self.aggregation_specs.values()):
            expanded_request_table_name = self.request_table_plan.get_expanded_request_table_name(
                agg_spec
            )
            agg_result_name = agg_spec.agg_result_name
            agg_sql = self.construct_aggregation_sql(
                expanded_request_table_name=expanded_request_table_name,
                tile_table_id=agg_spec.tile_table_id,
                point_in_time_column=point_in_time_column,
                entity_ids=agg_spec.entity_ids,
                merge_expr=agg_spec.merge_expr,
                agg_result_name=agg_result_name,
            )
            agg_table_alias = f"T{i}"
            agg_result_name_alias = (
                f'"{agg_table_alias}"."{agg_result_name}" AS "{agg_result_name}"'
            )
            qualified_aggregation_names.append(agg_result_name_alias)
            join_conditions_lst = [
                f"REQ.{point_in_time_column} = {agg_table_alias}.{point_in_time_column}",
            ]
            for k in agg_spec.entity_ids:
                join_conditions_lst += [f"REQ.{k} = {agg_table_alias}.{k}"]
            join_conditions = " AND ".join(join_conditions_lst)
            left_joins.append(
                f"""
            LEFT JOIN (
                {agg_sql}
            ) {agg_table_alias}
            ON {join_conditions}
                """
            )
        left_joins_sql = "\n".join(left_joins)
        qualified_aggregation_names_str = ", ".join(qualified_aggregation_names)
        combined_sql = (
            f"""
            SELECT
                REQ.*,
                {qualified_aggregation_names_str}
            FROM {REQUEST_TABLE_NAME} REQ
            """
            + left_joins_sql
        )
        combined_sql = prettify_sql(combined_sql)
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
            feature_alias = f'{feature_spec.feature_expr} AS "{feature_spec.feature_name}"'
            qualified_feature_names.append(feature_alias)
        request_table_column_names = ", ".join([f'AGG."{col}"' for col in request_table_columns])
        qualified_feature_names_str = ", ".join(qualified_feature_names)
        sql = f"""
            SELECT
                {request_table_column_names},
                {qualified_feature_names_str}
            FROM {self.AGGREGATION_TABLE_NAME} AGG
            """
        sql = prettify_sql(sql)
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
        cte_statements.append(self.construct_combined_aggregation_cte(point_in_time_column))
        cte_sql = construct_cte_sql(cte_statements)

        post_aggregation_sql = self.construct_post_aggregation_sql(request_table_columns)
        sql = "\n".join([cte_sql, post_aggregation_sql])
        return sql


class FeatureExecutionPlanner:
    """Responsible for constructing a FeatureExecutionPlan given QueryGraph and Node

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    """

    def __init__(self, graph: QueryGraph):
        self.graph = graph
        self.plan = FeatureExecutionPlan()

    def generate_plan(self, node: Node) -> FeatureExecutionPlan:
        """Generate FeatureExecutionPlan for given query graph Node

        Parameters
        ----------
        node : Node
            Query graph node

        Returns
        -------
        FeatureExecutionPlan
        """
        for groupby_node in find_parent_groupby_nodes(self.graph, node):
            self.parse_and_update_specs_from_groupby(groupby_node)
        self.update_feature_specs(node)
        return self.plan

    def parse_and_update_specs_from_groupby(self, groupby_node: Node) -> None:
        """Update FeatureExecutionPlan with a groupby query node

        Parameters
        ----------
        groupby_node : Node
            Groupby query node
        """
        agg_specs = AggregationSpec.from_groupby_query_node(self.graph, groupby_node)
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

        elif isinstance(sql_node, Project):
            feature_name = sql_node.column_name
            feature_expr = sql_node.sql.sql()
            feature_spec = FeatureSpec(feature_name=feature_name, feature_expr=feature_expr)
            self.plan.add_feature_spec(feature_spec)

        else:
            # Otherwise, there is no way to know about the feature name. Technically speaking this
            # could still be previewed as an "unnamed" feature since the expression is available,
            # but it cannot be published.
            feature_name = "Unnamed"
            feature_expr = sql_node.sql.sql()
            feature_spec = FeatureSpec(feature_name=feature_name, feature_expr=feature_expr)
            self.plan.add_feature_spec(feature_spec)


def construct_expanded_request_table_sql(
    window_size: int,
    frequency: int,
    blind_spot: int,
    time_modulo_frequency: int,
    entity_columns: list[str],
) -> str:
    """Construct SQL for expanded SQLs

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
    entity_columns : list[str]
        List of entity columns

    Returns
    -------
    str
        SQL code for expanding request table
    """
    select_entity_columns = ", ".join([f"REQ.{col}" for col in entity_columns])
    sql = f"""
    SELECT
        REQ.POINT_IN_TIME,
        {select_entity_columns},
        T.value AS REQ_TILE_INDEX
    FROM REQUEST_TABLE REQ,
    Table(
        Flatten(
            SELECT COMPUTE_TILE_INDICES(
                DATE_PART(epoch, REQ.POINT_IN_TIME),
                {window_size},
                {frequency},
                {blind_spot},
                {time_modulo_frequency}
            )
        )
    ) T
    """
    return sql


def construct_cte_sql(cte_statements: list[tuple[str, str]]) -> str:
    """Construct CTEs section of a SQL code

    Parameters
    ----------
    cte_statements : list[tuple[str, str]]
        List of CTE statements

    Returns
    -------
    str
    """
    cte_definitions = []
    for table_name, table_statement in cte_statements:
        cte_definitions.append(f"{table_name} AS ({table_statement})")
    cte_sql = ",\n".join(cte_definitions)
    cte_sql = f"WITH {cte_sql}"
    return cte_sql


def get_feature_preview_sql(
    graph: QueryGraph,
    node: Node,
    entity_columns: list[str],
    point_in_time_and_entity_id: dict[str, Any],
) -> str:
    """Get SQL code for previewing SQL

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    node : Node
        Query graph node
    entity_columns : list[str]
        Entity columns
    point_in_time_and_entity_id : dict
        Preview request consisting of point in time and entity ID(s)

    Returns
    -------
    str

    Raises
    ------
    KeyError
        If any required entity columns is not provided
    """

    point_in_time = point_in_time_and_entity_id["POINT_IN_TIME"]
    for col in entity_columns:
        if col not in point_in_time_and_entity_id:
            raise KeyError(f"Entity column not provided: {col}")

    cte_statements = []

    planner = FeatureExecutionPlanner(graph)
    execution_plan = planner.generate_plan(node)

    # build required tiles
    logger.debug("Building required tiles")
    tic = time.time()
    on_demand_tile_ctes = construct_on_demand_tile_ctes(graph, node, point_in_time)
    cte_statements.extend(on_demand_tile_ctes)
    elapsed = time.time() - tic
    logger.debug(f"Building required tiles took {elapsed:.2}s")

    # prepare request table
    logger.debug("Uploading request table")
    tic = time.time()
    df_request = pd.DataFrame([point_in_time_and_entity_id])
    request_table_sql = construct_preview_request_table_sql(df_request, ["POINT_IN_TIME"])
    cte_statements.append((REQUEST_TABLE_NAME, request_table_sql))
    elapsed = time.time() - tic
    logger.debug(f"Uploading request table took {elapsed:.2}s")

    logger.debug("Generating full feature SQL")
    tic = time.time()
    preview_sql = execution_plan.construct_combined_sql(
        point_in_time_column="POINT_IN_TIME",
        request_table_columns=df_request.columns.tolist(),
        prior_cte_statements=cte_statements,
    )
    elapsed = time.time() - tic
    logger.debug(f"Generating full SQL took {elapsed:.2}s")
    logger.debug(f"Feature SQL:\n{preview_sql}")

    return preview_sql
