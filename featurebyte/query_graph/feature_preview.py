"""
Feature preview SQL generation
"""
from __future__ import annotations

from typing import Any

import time

import pandas as pd
from sqlglot import select

from featurebyte.enum import SpecialColumnName
from featurebyte.logger import logger
from featurebyte.query_graph.feature_common import REQUEST_TABLE_NAME, prettify_sql
from featurebyte.query_graph.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.query_graph.sql import escape_column_name
from featurebyte.query_graph.tile_compute import construct_on_demand_tile_ctes


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
            assert isinstance(col, str)
            if col in date_cols:
                expr = f"CAST('{str(value)}' AS TIMESTAMP)"
            else:
                if isinstance(value, str):
                    expr = f"'{value}'"
                else:
                    expr = value
            columns.append(f"{expr} AS {escape_column_name(col)}")
        row_sql = select(*columns).sql()
        row_sqls.append(row_sql)
    return prettify_sql(" UNION ALL\n".join(row_sqls))


def get_feature_preview_sql(
    graph: QueryGraph,
    nodes: list[Node],
    point_in_time_and_serving_name: dict[str, Any],
) -> str:
    """Get SQL code for previewing SQL

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    nodes : list[Node]
        List of query graph node
    point_in_time_and_serving_name : dict
        Dictionary consisting the point in time and entity ids based on which the feature
        preview will be computed

    Returns
    -------
    str
    """

    point_in_time = point_in_time_and_serving_name[SpecialColumnName.POINT_IN_TIME]

    cte_statements = []

    planner = FeatureExecutionPlanner(graph)
    execution_plan = planner.generate_plan(nodes)

    # build required tiles
    tic = time.time()
    for node in nodes:
        on_demand_tile_ctes = construct_on_demand_tile_ctes(graph, node, point_in_time)
        cte_statements.extend(on_demand_tile_ctes)
    cte_statements = list(set(cte_statements))
    elapsed = time.time() - tic
    logger.debug(f"Constructing required tiles SQL took {elapsed:.2}s")

    # prepare request table
    tic = time.time()
    df_request = pd.DataFrame([point_in_time_and_serving_name])
    request_table_sql = construct_preview_request_table_sql(
        df_request, [SpecialColumnName.POINT_IN_TIME]
    )
    cte_statements.append((REQUEST_TABLE_NAME, request_table_sql))
    elapsed = time.time() - tic
    logger.debug(f"Constructing request table SQL took {elapsed:.2}s")

    tic = time.time()
    preview_sql = execution_plan.construct_combined_sql(
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=df_request.columns.tolist(),
        prior_cte_statements=cte_statements,
    )
    elapsed = time.time() - tic
    logger.debug(f"Generating full SQL took {elapsed:.2}s")
    logger.debug(f"Feature SQL:\n{preview_sql}")

    return preview_sql
