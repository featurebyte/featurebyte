"""
Target forward aggregate sql generation
"""
from __future__ import annotations

from typing import Any, List, Optional, cast

import time

import pandas as pd

from featurebyte import SourceType, get_logger
from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.common import CteStatement, sql_to_string
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner

logger = get_logger(__name__)


def get_forward_aggregate_sql(
    request_table_name: str,
    graph: QueryGraphModel,
    nodes: list[Node],
    source_type: SourceType,
    point_in_time_and_serving_name_list: Optional[list[dict[str, Any]]] = None,
) -> str:
    """
    Get SQL code for target forward aggregate SQL

    Parameters
    ----------
    request_table_name : str
        Name of request table to use
    graph : QueryGraphModel
        Query graph model
    nodes : list[Node]
        List of query graph node
    source_type : SourceType
        Source type information
    point_in_time_and_serving_name_list : Optional[list[dict[str, Any]]]
        List of Dictionary consisting the point in time and entity ids based on which the feature
        preview will be computed

    Returns
    -------
    str
    """
    planner = FeatureExecutionPlanner(
        graph,
        source_type=source_type,
        is_online_serving=False,
    )
    execution_plan = planner.generate_plan(nodes)

    cte_statements: Optional[List[CteStatement]] = None
    request_table_columns: Optional[List[str]] = None

    if point_in_time_and_serving_name_list:
        # prepare request table
        tic = time.time()
        df_request = pd.DataFrame(point_in_time_and_serving_name_list)
        request_table_sql = construct_dataframe_sql_expr(
            df_request, [SpecialColumnName.POINT_IN_TIME]
        )
        cte_statements = [(request_table_name, request_table_sql)]
        request_table_columns = cast(List[str], df_request.columns.tolist())

        elapsed = time.time() - tic
        logger.debug(f"Constructing request table SQL took {elapsed:.2}s")

    tic = time.time()
    preview_sql = sql_to_string(
        execution_plan.construct_combined_sql(
            request_table_name=request_table_name,
            point_in_time_column=SpecialColumnName.POINT_IN_TIME,
            request_table_columns=request_table_columns,
            prior_cte_statements=cte_statements,
        ),
        source_type=source_type,
    )
    elapsed = time.time() - tic
    logger.debug(f"Generating full SQL took {elapsed:.2}s")

    return preview_sql
