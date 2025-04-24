"""
Helpers for SQL generation related to cron feature jobs
"""

from dataclasses import dataclass
from typing import Optional

from pandas import DataFrame
from sqlglot import expressions

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import TimeSeriesWindowAggregateNode
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


def get_cron_feature_job_settings(
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
    for node in nodes:
        for time_series_agg_node in graph.iterate_nodes(
            node, NodeType.TIME_SERIES_WINDOW_AGGREGATE
        ):
            assert isinstance(time_series_agg_node, TimeSeriesWindowAggregateNode)
            cron_feature_job_setting = time_series_agg_node.parameters.feature_job_setting.copy()
            # Deduplicate cron feature job settings. The actual request table name is not important.
            key = get_request_table_with_job_schedule_name(
                "request_table", cron_feature_job_setting
            )
            if key not in cron_feature_job_settings:
                cron_feature_job_settings[key] = cron_feature_job_setting
    return list(cron_feature_job_settings.values())


def get_request_table_with_job_schedule_name(
    request_table_name: str, feature_job_setting: CronFeatureJobSetting
) -> str:
    """
    Get the name of the request table with the cron job schedule

    Parameters
    ----------
    request_table_name: str
        Request table name
    feature_job_setting: CronFeatureJobSetting
        Cron feature job setting to simulate

    Returns
    -------
    str
        Request table name with the cron job schedule
    """
    return f"{request_table_name}_{feature_job_setting.get_cron_expression_with_timezone()}"


def get_request_table_joined_job_schedule_expr(
    request_table_name: str,
    request_table_columns: list[str],
    job_schedule_table_name: str,
    adapter: BaseAdapter,
) -> expressions.Select:
    """
    Get the sql expression that constructs a new request table by joining each row in the request
    table to the latest row in the cron job schedule table where the job datetime is before the
    point in time.

    Parameters
    ----------
    request_table_name: str
        Request table name
    request_table_columns: list[str]
        Request table columns
    job_schedule_table_name: str
        Job schedule table name
    adapter: BaseAdapter
        SQL adapter

    Returns
    -------
    expressions.Select
    """
    left_table = Table(
        expr=quoted_identifier(request_table_name),
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
        output_columns=[InternalName.CRON_JOB_SCHEDULE_DATETIME],
    )
    joined_expr = get_scd_join_expr(
        left_table=left_table,
        right_table=right_table,
        join_type="left",
        adapter=adapter,
        allow_exact_match=False,
    )
    return joined_expr
