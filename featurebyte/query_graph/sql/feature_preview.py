"""
Feature or Target preview SQL generation
"""

from __future__ import annotations

import time
from typing import Any, List, Optional, cast

import pandas as pd

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.logging import get_logger
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.common import CteStatement, quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.cron import JobScheduleTableSet
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.sql.parent_serving import construct_request_table_with_parent_entities
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.tile_compute import OnDemandTileComputePlan

logger = get_logger(__name__)


def get_feature_or_target_preview_sql(
    request_table_name: str,
    graph: QueryGraphModel,
    nodes: list[Node],
    source_info: SourceInfo,
    point_in_time_and_serving_name_list: Optional[list[dict[str, Any]]] = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    job_schedule_table_set: Optional[JobScheduleTableSet] = None,
) -> str:
    """
    Get SQL code for previewing SQL for features or targets.

    Parameters
    ----------
    request_table_name : str
        Name of request table to use
    graph : QueryGraphModel
        Query graph model
    nodes : list[Node]
        List of query graph node
    source_info: SourceInfo
        Source information
    point_in_time_and_serving_name_list : Optional[list[dict[str, Any]]]
        List of Dictionary consisting the point in time and entity ids based on which the feature
        preview will be computed
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    job_schedule_table_set: Optional[JobScheduleTableSet]
        Job schedule table set if available. These will be used to compute features that are using
        a cron-based feature job setting.

    Returns
    -------
    str
    """
    planner = FeatureExecutionPlanner(
        graph,
        source_info=source_info,
        is_online_serving=False,
        job_schedule_table_set=job_schedule_table_set,
    )
    execution_plan = planner.generate_plan(nodes)

    exclude_columns = set()
    cte_statements: List[CteStatement] = []
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

        if parent_serving_preparation is not None:
            parent_serving_result = construct_request_table_with_parent_entities(
                request_table_name=request_table_name,
                request_table_columns=request_table_columns,
                join_steps=parent_serving_preparation.join_steps,
                feature_store_details=parent_serving_preparation.feature_store_details,
            )
            request_table_name = parent_serving_result.new_request_table_name
            request_table_columns = parent_serving_result.new_request_table_columns
            cte_statements.append((request_table_name, parent_serving_result.table_expr))
            exclude_columns.update(parent_serving_result.parent_entity_columns)

        elapsed = time.time() - tic
        logger.debug(f"Constructing request table SQL took {elapsed:.2}s")

        # build required tiles
        tic = time.time()
        tile_compute_plan = OnDemandTileComputePlan(
            request_table_name=request_table_name,
            source_info=source_info,
        )
        for node in nodes:
            tile_compute_plan.process_node(graph, node)
        tile_compute_ctes = sorted(tile_compute_plan.construct_on_demand_tile_ctes())
        cte_statements.extend(tile_compute_ctes)
        elapsed = time.time() - tic
        logger.debug(f"Constructing required tiles SQL took {elapsed:.2}s")

    if job_schedule_table_set is not None:
        for job_schedule_table in job_schedule_table_set.tables:
            assert job_schedule_table.job_schedule_dataframe is not None
            job_schedule_table_sql = construct_dataframe_sql_expr(
                job_schedule_table.job_schedule_dataframe,
                [
                    InternalName.CRON_JOB_SCHEDULE_DATETIME,
                    InternalName.CRON_JOB_SCHEDULE_DATETIME_UTC,
                ],
            )
            cte_statements.append((
                quoted_identifier(job_schedule_table.table_name),
                job_schedule_table_sql,
            ))

    tic = time.time()
    preview_sql = sql_to_string(
        execution_plan.construct_combined_sql(
            request_table_name=request_table_name,
            point_in_time_column=SpecialColumnName.POINT_IN_TIME,
            request_table_columns=request_table_columns,
            prior_cte_statements=cte_statements,
            exclude_columns=exclude_columns,
        ),
        source_type=source_info.source_type,
    )
    elapsed = time.time() - tic
    logger.debug(f"Generating full SQL took {elapsed:.2}s")

    return preview_sql
