"""
Deployment SQL generation
"""

from __future__ import annotations

from typing import Optional

from sqlglot import expressions

from featurebyte.enum import SpecialColumnName
from featurebyte.models.column_statistics import ColumnStatisticsInfo
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.aggregator.base import CommonTable
from featurebyte.query_graph.sql.common import PartitionColumnFilters
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner, FeatureQueryPlan
from featurebyte.query_graph.sql.online_serving import construct_request_table_query
from featurebyte.query_graph.sql.source_info import SourceInfo


def get_deployment_feature_query_plan(
    graph: QueryGraph,
    nodes: list[Node],
    source_info: SourceInfo,
    request_table_columns: list[str],
    request_table_expr: expressions.Select,
    point_in_time_placeholder: expressions.Expression,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    column_statistics_info: Optional[ColumnStatisticsInfo] = None,
    partition_column_filters: Optional[PartitionColumnFilters] = None,
) -> FeatureQueryPlan:
    """
    Construct SQL code that can be used to lookup pre-computed features from online store

    Parameters
    ----------
    graph: QueryGraph
        Query graph
    nodes: list[Node]
        List of query graph nodes
    source_info: SourceInfo
        Source information
    request_table_columns: list[str]
        Request table columns
    request_table_expr: Optional[expressions.Select]
        Select statement for the request table
    point_in_time_placeholder: expressions.Expression
        Point in time placeholder expression
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    column_statistics_info: Optional[ColumnStatisticsInfo]
        Column statistics information
    partition_column_filters: Optional[PartitionColumnFilters]
        Partition column filters to apply to source tables

    Returns
    -------
    FeatureQueryPlan
    """
    planner = FeatureExecutionPlanner(
        graph,
        source_info=source_info,
        is_online_serving=False,
        parent_serving_preparation=parent_serving_preparation,
        column_statistics_info=column_statistics_info,
        partition_column_filters=partition_column_filters,
        is_deployment_sql=True,
    )
    plan = planner.generate_plan(nodes)

    # Form a request table as a common table expression (CTE) and add the point in time column
    request_query = construct_request_table_query(
        current_timestamp_expr=point_in_time_placeholder,
        request_table_columns=request_table_columns,
        request_table_expr=request_table_expr,
    )
    request_table_name = "DEPLOYMENT_REQUEST_TABLE"
    ctes = [CommonTable(request_table_name, request_query, quoted=False)]

    output = plan.construct_combined_sql(
        request_table_name=request_table_name,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=request_table_columns,
        prior_cte_statements=ctes,
        exclude_columns={SpecialColumnName.POINT_IN_TIME},
    )

    return output
