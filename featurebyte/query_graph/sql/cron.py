"""
Helpers for SQL generation related to cron feature jobs
"""

from dataclasses import dataclass
from typing import Optional, Tuple

from pandas import DataFrame
from sqlglot import expressions

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    AggregateAsAtNode,
    LookupNode,
    TimeSeriesWindowAggregateNode,
)
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.scd_helper import Table, get_scd_join_expr


@dataclass
class JobScheduleTable:
    """
    Represents a registered job schedule table
    """

    table_name: str
    cron_feature_job_setting: CronFeatureJobSetting
    job_schedule_dataframe: Optional[DataFrame] = None


@dataclass
class JobScheduleTableSet:
    """
    Represents a collection of registered job schedule tables
    """

    tables: list[JobScheduleTable]


def get_all_cron_feature_job_settings(
    graph: QueryGraphModel, nodes: list[Node]
) -> list[CronFeatureJobSetting]:
    """
    Get the unique cron feature job settings from the time series window aggregate nodes

    Parameters
    ----------
    graph: QueryGraphModel
        Query graph
    nodes: list[Node]
        List of nodes

    Returns
    -------
    list[CronFeatureJobSetting]
    """
    result = []
    for node in nodes:
        for time_series_agg_node in graph.iterate_nodes(
            node, NodeType.TIME_SERIES_WINDOW_AGGREGATE
        ):
            assert isinstance(time_series_agg_node, TimeSeriesWindowAggregateNode)
            result.append(time_series_agg_node.parameters.feature_job_setting)

        for lookup_node in graph.iterate_nodes(node, NodeType.LOOKUP):
            assert isinstance(lookup_node, LookupNode)
            snapshots_parameters = lookup_node.parameters.snapshots_parameters
            if snapshots_parameters is not None:
                if snapshots_parameters.feature_job_setting is not None:
                    result.append(snapshots_parameters.feature_job_setting)

        for aggregate_asat_node in graph.iterate_nodes(node, NodeType.AGGREGATE_AS_AT):
            assert isinstance(aggregate_asat_node, AggregateAsAtNode)
            snapshots_parameters = aggregate_asat_node.parameters.snapshots_parameters
            if snapshots_parameters is not None:
                if snapshots_parameters.feature_job_setting is not None:
                    result.append(snapshots_parameters.feature_job_setting)

    return result


def get_unique_cron_feature_job_settings(
    graph: QueryGraphModel, nodes: list[Node]
) -> list[CronFeatureJobSetting]:
    """
    Get the unique cron feature job settings from the time series window aggregate nodes

    Parameters
    ----------
    graph: QueryGraphModel
        Query graph
    nodes: list[Node]
        List of nodes

    Returns
    -------
    list[CronFeatureJobSetting]
    """
    cron_feature_job_settings = {}
    for cron_feature_job_setting in get_all_cron_feature_job_settings(graph, nodes):
        key = get_request_table_job_datetime_column_name(cron_feature_job_setting)
        if key not in cron_feature_job_settings:
            cron_feature_job_settings[key] = cron_feature_job_setting
    return list(cron_feature_job_settings.values())


def get_request_table_job_datetime_column_name(feature_job_setting: CronFeatureJobSetting) -> str:
    """
    Get the name of the job datetime column for the cron job schedule when joined to the request
    table based on point in time.

    Parameters
    ----------
    feature_job_setting: CronFeatureJobSetting
        Cron feature job setting
    """
    return f"{InternalName.CRON_JOB_SCHEDULE_DATETIME}_{feature_job_setting.get_cron_expression_with_timezone()}"


def get_request_table_joined_job_schedule_expr(
    request_table_expr: expressions.Select,
    request_table_columns: list[str],
    job_schedule_table_name: str,
    job_datetime_output_column_name: str,
    adapter: BaseAdapter,
) -> expressions.Select:
    """
    Get the sql expression that constructs a new request table by joining each row in the request
    table to the latest row in the cron job schedule table where the job datetime is before the
    point in time.

    Parameters
    ----------
    request_table_expr: expressions.Select
        Request table expression
    request_table_columns: list[str]
        Request table columns
    job_schedule_table_name: str
        Job schedule table name
    job_datetime_output_column_name: str
        Output column name for the job datetime column
    adapter: BaseAdapter
        SQL adapter

    Returns
    -------
    expressions.Select
    """
    left_table = Table(
        expr=request_table_expr,
        timestamp_column=SpecialColumnName.POINT_IN_TIME,
        timestamp_schema=None,
        join_keys=[],
        input_columns=request_table_columns,
        output_columns=request_table_columns,
    )
    right_table = Table(
        expr=quoted_identifier(job_schedule_table_name),
        timestamp_column=InternalName.CRON_JOB_SCHEDULE_DATETIME_UTC,
        timestamp_schema=None,
        join_keys=[],
        input_columns=[InternalName.CRON_JOB_SCHEDULE_DATETIME],
        output_columns=[job_datetime_output_column_name],
    )
    joined_expr = get_scd_join_expr(
        left_table=left_table,
        right_table=right_table,
        join_type="left",
        adapter=adapter,
        allow_exact_match=False,
    )
    return joined_expr


def get_request_table_join_all_job_schedules_expr(
    request_table_name: str,
    request_table_columns: list[str],
    job_schedule_table_set: JobScheduleTableSet,
    adapter: BaseAdapter,
) -> Tuple[expressions.Select, list[str]]:
    """
    Get the request table expanded with job schedule datetime columns

    Parameters
    ----------
    request_table_name: str
        Request table name
    request_table_columns: list[str]
        Columns in the request table
    job_schedule_table_set: JobScheduleTableSet
        Set of job schedule tables. Each table corresponds to a unique cron feature job setting and
        should result in an additional job datetime column in the request table
    adapter: BaseAdapter
        SQL adapter

    Returns
    -------
    Tuple[expressions.Select, list[str]]
        SQL expression for the expanded request table and list of job datetime columns added
    """
    current_request_table_expr = expressions.select(*[
        quoted_identifier(col) for col in request_table_columns
    ]).from_(quoted_identifier(request_table_name))

    current_request_table_columns = request_table_columns[:]
    for job_schedule_table in job_schedule_table_set.tables:
        job_datetime_output_column_name = get_request_table_job_datetime_column_name(
            job_schedule_table.cron_feature_job_setting
        )
        current_request_table_expr = get_request_table_joined_job_schedule_expr(
            request_table_expr=current_request_table_expr,
            request_table_columns=request_table_columns,
            job_schedule_table_name=job_schedule_table.table_name,
            job_datetime_output_column_name=job_datetime_output_column_name,
            adapter=adapter,
        )
        current_request_table_columns.append(job_datetime_output_column_name)

    job_datetime_columns = current_request_table_columns[len(request_table_columns) :]
    return current_request_table_expr, job_datetime_columns
