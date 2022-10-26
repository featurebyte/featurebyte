"""
Feature preview SQL generation
"""
# pylint: disable=too-many-locals
from __future__ import annotations

from typing import Any, Optional

import time

import pandas as pd

from featurebyte.enum import SourceType, SpecialColumnName
from featurebyte.logger import logger
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME, sql_to_string
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.sql.tile_compute import OnDemandTileComputePlan


def get_feature_preview_sql(
    graph: QueryGraph,
    nodes: list[Node],
    source_type: SourceType,
    point_in_time_and_serving_name: Optional[dict[str, Any]] = None,
) -> str:
    """Get SQL code for previewing SQL

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    nodes : list[Node]
        List of query graph node
    source_type : SourceType
        Source type information
    point_in_time_and_serving_name : Optional[dict[str, Any]]
        Dictionary consisting the point in time and entity ids based on which the feature
        preview will be computed

    Returns
    -------
    str
    """
    planner = FeatureExecutionPlanner(graph, source_type=source_type)
    execution_plan = planner.generate_plan(nodes)

    if point_in_time_and_serving_name:
        # build required tiles
        tic = time.time()
        point_in_time = point_in_time_and_serving_name[SpecialColumnName.POINT_IN_TIME]
        tile_compute_plan = OnDemandTileComputePlan(point_in_time, source_type=source_type)
        for node in nodes:
            tile_compute_plan.process_node(graph, node)
        cte_statements = sorted(tile_compute_plan.construct_on_demand_tile_ctes())
        elapsed = time.time() - tic
        logger.debug(f"Constructing required tiles SQL took {elapsed:.2}s")

        # prepare request table
        tic = time.time()
        df_request = pd.DataFrame([point_in_time_and_serving_name])
        request_table_sql = construct_dataframe_sql_expr(
            df_request, [SpecialColumnName.POINT_IN_TIME]
        )
        cte_statements.append((REQUEST_TABLE_NAME, request_table_sql))
        elapsed = time.time() - tic
        logger.debug(f"Constructing request table SQL took {elapsed:.2}s")

        request_table_columns = df_request.columns.tolist()
    else:
        cte_statements = None
        request_table_columns = None

    tic = time.time()
    preview_sql = sql_to_string(
        execution_plan.construct_combined_sql(
            point_in_time_column=SpecialColumnName.POINT_IN_TIME,
            request_table_columns=request_table_columns,
            prior_cte_statements=cte_statements,
        ),
        source_type=source_type,
    )
    elapsed = time.time() - tic
    logger.debug(f"Generating full SQL took {elapsed:.2}s")
    logger.debug(f"Feature SQL:\n{preview_sql}")

    return preview_sql
