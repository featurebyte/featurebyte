"""
SQL generation for online serving
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union, cast

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import Expression, select

from featurebyte.common.utils import prepare_dataframe_for_json
from featurebyte.enum import InternalName, SourceType, SpecialColumnName
from featurebyte.logging import get_logger
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.column_statistics import ColumnStatisticsInfo
from featurebyte.models.feature_query_set import FeatureQueryGenerator, FeatureQuerySet
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.models.tile import OnDemandTileTable
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.aggregator.base import CommonTable
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.batch_helper import (
    get_feature_names,
)
from featurebyte.query_graph.sql.common import (
    REQUEST_TABLE_NAME,
    PartitionColumnFilters,
    get_fully_qualified_table_name,
    get_qualified_column_identifier,
)
from featurebyte.query_graph.sql.cron import JobScheduleTableSet, get_cron_feature_job_settings
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.entity import (
    DUMMY_ENTITY_COLUMN_NAME,
    DUMMY_ENTITY_VALUE,
    get_combined_serving_names,
    get_combined_serving_names_expr,
)
from featurebyte.query_graph.sql.feature_compute import (
    FeatureExecutionPlanner,
    FeatureQuery,
    FeatureQueryPlan,
)
from featurebyte.query_graph.sql.online_serving_util import get_version_placeholder
from featurebyte.query_graph.sql.partition_filter_helper import get_partition_filters_from_graph
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.template import SqlExpressionTemplate

if TYPE_CHECKING:
    from featurebyte.service.column_statistics import ColumnStatisticsService
    from featurebyte.service.cron_helper import CronHelper

from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.session.session_helper import SessionHandler, execute_feature_query_set

logger = get_logger(__name__)

PROGRESS_MESSAGE_COMPUTING_ONLINE_FEATURES = "Computing online features"


def get_aggregation_result_names(
    graph: QueryGraph, nodes: list[Node], source_info: SourceInfo
) -> list[str]:
    """
    Get a list of aggregation result names that correspond to the graph and nodes

    Parameters
    ----------
    graph: QueryGraph
        Query graph
    nodes: list[Node]
        List of query graph nodes
    source_info: SourceInfo
        Source type information

    Returns
    -------
    list[str]
    """
    planner = FeatureExecutionPlanner(
        graph,
        source_info=source_info,
        is_online_serving=True,
    )
    plan = planner.generate_plan(nodes)
    return plan.tile_based_aggregation_result_names


def fill_version_placeholders(
    template_expr: Expression, versions: Dict[str, int]
) -> expressions.Select:
    """
    Fill the version placeholders in the SQL template

    Parameters
    ----------
    template_expr: Expression
        Retrieval query template with placeholders to be filled
    versions : Dict[str, int]
        Mapping from aggregation result name to version

    Returns
    -------
    expressions.Select
    """
    placeholders_mapping = {
        get_version_placeholder(agg_result_name): version
        for (agg_result_name, version) in versions.items()
    }
    return cast(
        expressions.Select,
        SqlExpressionTemplate(template_expr).render(placeholders_mapping, as_str=False),
    )


def construct_request_table_query(
    current_timestamp_expr: Expression,
    request_table_columns: list[str],
    request_table_expr: Optional[expressions.Select] = None,
    request_table_details: Optional[TableDetails] = None,
) -> expressions.Select:
    """
    Construct a Select expression for the request table

    Parameters
    ----------
    current_timestamp_expr: Expression
        The sql expression to use for the point-in-time value
    request_table_columns: list[str]
        Request table columns
    request_table_expr: Optional[expressions.Select]
        Select statement for the request table
    request_table_details: Optional[TableDetails]
        Location of the request table in the data warehouse

    Returns
    -------
    expressions.Select
    """
    expr = select(*[get_qualified_column_identifier(col, "REQ") for col in request_table_columns])
    expr = expr.select(
        expressions.alias_(current_timestamp_expr, alias=SpecialColumnName.POINT_IN_TIME)
    )
    request_table_columns.append(SpecialColumnName.POINT_IN_TIME)
    if request_table_expr is not None:
        expr = expr.from_(request_table_expr.subquery(alias="REQ"))
    else:
        assert request_table_details is not None
        expr = expr.from_(
            get_fully_qualified_table_name(request_table_details.model_dump(), alias="REQ")
        )
    return expr


def get_online_store_retrieval_expr(
    graph: QueryGraph,
    nodes: list[Node],
    source_info: SourceInfo,
    current_timestamp_expr: Expression,
    request_table_columns: list[str],
    request_table_expr: Optional[expressions.Select] = None,
    request_table_details: Optional[TableDetails] = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    job_schedule_table_set: Optional[JobScheduleTableSet] = None,
    on_demand_tile_tables: Optional[List[OnDemandTileTable]] = None,
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
    current_timestamp_expr: Expression
        The sql expression to use for the point-in-time value
    request_table_expr: Optional[expressions.Select]
        Select statement for the request table
    request_table_details: Optional[TableDetails]
        Location of the request table in the data warehouse
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    job_schedule_table_set: Optional[JobScheduleTableSet]
        Job schedule table set if available. These will be used to compute features that are using
        a cron-based feature job setting.
    on_demand_tile_tables: Optional[List[OnDemandTileTable]]
        List of on-demand tile tables to be used in the query
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
        job_schedule_table_set=job_schedule_table_set,
        on_demand_tile_tables=on_demand_tile_tables,
        column_statistics_info=column_statistics_info,
        partition_column_filters=partition_column_filters,
    )
    plan = planner.generate_plan(nodes)

    # Form a request table as a common table expression (CTE) and add the point in time column
    request_query = construct_request_table_query(
        current_timestamp_expr=current_timestamp_expr,
        request_table_columns=request_table_columns,
        request_table_expr=request_table_expr,
        request_table_details=request_table_details,
    )
    request_table_name = "ONLINE_" + REQUEST_TABLE_NAME
    ctes = [CommonTable(request_table_name, request_query, quoted=False)]

    output = plan.construct_combined_sql(
        request_table_name=request_table_name,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=request_table_columns,
        prior_cte_statements=ctes,
        exclude_columns={SpecialColumnName.POINT_IN_TIME},
    )

    return output


def get_current_timestamp_expr(request_timestamp: datetime) -> Expression:
    """
    Get the sql expression to use for the current timestamp

    Parameters
    ----------
    request_timestamp: datetime
        The timestamp value to use as the point-in-time

    Returns
    -------
    Expression
    """
    if request_timestamp.tzinfo is not None:
        request_timestamp = request_timestamp.astimezone(timezone.utc).replace(tzinfo=None)
    current_timestamp_expr = make_literal_value(request_timestamp, cast_as_timestamp=True)
    return current_timestamp_expr


def add_concatenated_serving_names(
    select_expr: expressions.Select,
    concatenate_serving_names: Optional[list[str]],
    source_type: SourceType,
    serving_names_table_alias: Optional[str] = None,
) -> expressions.Select:
    """
    Add concatenated serving name column to the provided Select statement which is assumed to
    contain all the serving names.

    Parameters
    ----------
    select_expr: expressions.Select
        Select statement
    concatenate_serving_names: Optional[list[str]]
        List of serving names to concatenate
    source_type: SourceType
        Source type information
    serving_names_table_alias: Optional[str]
        Table alias for the serving names. Serving names will not be table qualified if not
        provided.

    Returns
    -------
    expressions.Select
    """
    if concatenate_serving_names is None:
        return select_expr
    if len(concatenate_serving_names) > 1:
        updated_select_expr = select_expr.select(
            expressions.alias_(
                get_combined_serving_names_expr(
                    concatenate_serving_names, serving_names_table_alias=serving_names_table_alias
                ),
                alias=get_combined_serving_names(concatenate_serving_names),
                quoted=True,
            )
        )
    elif source_type == SourceType.DATABRICKS_UNITY and len(concatenate_serving_names) == 0:
        updated_select_expr = select_expr.select(
            expressions.alias_(
                make_literal_value(DUMMY_ENTITY_VALUE),
                alias=DUMMY_ENTITY_COLUMN_NAME,
                quoted=True,
            )
        )
    else:
        updated_select_expr = select_expr
    return updated_select_expr


class OnlineFeatureQueryGenerator(FeatureQueryGenerator):
    """
    Online feature query generator
    """

    def __init__(
        self,
        graph: QueryGraph,
        nodes: list[Node],
        source_info: SourceInfo,
        current_timestamp_expr: Expression,
        versions: Dict[str, int],
        request_table_columns: list[str],
        request_table_expr: Optional[expressions.Select] = None,
        request_table_details: Optional[TableDetails] = None,
        parent_serving_preparation: Optional[ParentServingPreparation] = None,
        job_schedule_table_set: Optional[JobScheduleTableSet] = None,
        concatenate_serving_names: Optional[list[str]] = None,
        on_demand_tile_tables: Optional[list[OnDemandTileTable]] = None,
        column_statistics_info: Optional[ColumnStatisticsInfo] = None,
        partition_column_filters: Optional[PartitionColumnFilters] = None,
    ):
        self.graph = graph
        self.nodes = nodes
        self.source_info = source_info
        self.current_timestamp_expr = current_timestamp_expr
        self.versions = versions
        self.request_table_columns = request_table_columns
        self.request_table_expr = request_table_expr
        self.request_table_details = request_table_details
        self.parent_serving_preparation = parent_serving_preparation
        self.job_schedule_table_set = job_schedule_table_set
        self.concatenate_serving_names = concatenate_serving_names
        self.on_demand_tile_tables = on_demand_tile_tables
        self.column_statistics_info = column_statistics_info
        self.partition_column_filters = partition_column_filters

    def get_query_graph(self) -> QueryGraph:
        return self.graph

    def get_nodes(self) -> list[Node]:
        return self.nodes

    def generate_feature_query(self, node_names: list[str], table_name: str) -> FeatureQuery:
        nodes = [self.graph.get_node_by_name(node_name) for node_name in node_names]
        feature_query_plan = get_online_store_retrieval_expr(
            graph=self.graph,
            nodes=nodes,
            current_timestamp_expr=self.current_timestamp_expr,
            request_table_columns=[InternalName.TABLE_ROW_INDEX.value] + self.request_table_columns,
            request_table_expr=self.request_table_expr,
            request_table_details=self.request_table_details,
            source_info=self.source_info,
            parent_serving_preparation=self.parent_serving_preparation,
            job_schedule_table_set=self.job_schedule_table_set,
            on_demand_tile_tables=self.on_demand_tile_tables,
            column_statistics_info=self.column_statistics_info,
            partition_column_filters=self.partition_column_filters,
        )
        feature_query_plan.transform(lambda x: fill_version_placeholders(x, self.versions))
        return feature_query_plan.get_feature_query(
            table_name=table_name, node_names=node_names, source_info=self.source_info
        )


class OnlineFeatureQuerySet(FeatureQuerySet):
    """
    Online feature query set

    The output query is modified to include concatenated serving names if specified
    """

    def construct_join_feature_sets_query(self) -> expressions.Select:
        """
        Construct the output query that joins the completed feature queries

        Returns
        -------
        expressions.Select
        """
        assert isinstance(self.feature_query_generator, OnlineFeatureQueryGenerator)
        output_expr = super().construct_join_feature_sets_query()
        return add_concatenated_serving_names(
            output_expr,
            self.feature_query_generator.concatenate_serving_names,
            self.feature_query_generator.source_info.source_type,
            serving_names_table_alias="REQ",
        )


def get_online_features_query_set(
    graph: QueryGraph,
    nodes: list[Node],
    versions: Dict[str, int],
    source_info: SourceInfo,
    request_table_columns: list[str],
    output_feature_names: list[str],
    request_table_name: str,
    request_timestamp: datetime,
    request_table_expr: Optional[expressions.Select] = None,
    request_table_details: Optional[TableDetails] = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    output_table_details: Optional[TableDetails] = None,
    output_include_row_index: bool = False,
    concatenate_serving_names: Optional[list[str]] = None,
    job_schedule_table_set: Optional[JobScheduleTableSet] = None,
    on_demand_tile_tables: Optional[List[OnDemandTileTable]] = None,
    column_statistics_info: Optional[ColumnStatisticsInfo] = None,
    partition_column_filters: Optional[PartitionColumnFilters] = None,
) -> FeatureQuerySet:
    """
    Construct a FeatureQuerySet object to compute the online features

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    nodes: list[list[Node]]
        List of query graph nodes
    versions: Dict[str, int]
        Versions of the result names in the online store
    request_table_columns : list[str]
        List of column names in the training events
    source_info: SourceInfo
        Source information
    output_table_details: TableDetails
        Output table details to write the results to
    output_feature_names : list[str]
        List of output feature names
    output_include_row_index: bool
        Whether to include the TABLE_ROW_INDEX column in the output
    request_table_name: str
        Name of the registered request table
    request_timestamp: datetime
        The timestamp value to use as the point-in-time
    request_table_expr: Optional[expressions.Select]
        Sql expression for the request table
    request_table_details: Optional[TableDetails]
        Location of the request table in the data warehouse
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    job_schedule_table_set: Optional[JobScheduleTableSet]
        Job schedule table set if available. These will be used to compute features that are using
        a cron-based feature job setting.
    concatenate_serving_names: Optional[list[str]]
        List of serving names to concatenate as a new column, if specified
    on_demand_tile_tables: Optional[List[OnDemandTileTable]]
        List of on-demand tile tables to be used in the query
    column_statistics_info: Optional[ColumnStatisticsInfo]
        Column statistics information
    partition_column_filters: Optional[PartitionColumnFilters]
        Partition column filters to apply to source tables

    Returns
    -------
    FeatureQuerySet
    """
    current_timestamp_expr = get_current_timestamp_expr(request_timestamp)
    feature_query_generator = OnlineFeatureQueryGenerator(
        graph=graph,
        nodes=nodes,
        source_info=source_info,
        current_timestamp_expr=current_timestamp_expr,
        versions=versions,
        request_table_columns=request_table_columns,
        request_table_expr=request_table_expr,
        request_table_details=request_table_details,
        parent_serving_preparation=parent_serving_preparation,
        job_schedule_table_set=job_schedule_table_set,
        concatenate_serving_names=concatenate_serving_names,
        on_demand_tile_tables=on_demand_tile_tables,
        column_statistics_info=column_statistics_info,
        partition_column_filters=partition_column_filters,
    )
    feature_query_set = OnlineFeatureQuerySet(
        feature_query_generator=feature_query_generator,
        request_table_name=request_table_name,
        request_table_columns=request_table_columns,
        output_table_details=output_table_details,
        output_feature_names=output_feature_names,
        output_include_row_index=output_include_row_index,
        progress_message=PROGRESS_MESSAGE_COMPUTING_ONLINE_FEATURES,
    )
    return feature_query_set


class TemporaryBatchRequestTable(FeatureByteBaseModel):
    """
    Temporary batch request table created manually without going through the standard table
    materialization task. Contains the essential information needed for batch features
    materialization.
    """

    column_names: List[str]
    table_details: TableDetails


async def get_online_features(
    session_handler: SessionHandler,
    cron_helper: CronHelper,
    column_statistics_service: ColumnStatisticsService,
    graph: QueryGraph,
    nodes: list[Node],
    request_data: Union[pd.DataFrame, BatchRequestTableModel, TemporaryBatchRequestTable],
    source_info: SourceInfo,
    online_store_table_version_service: OnlineStoreTableVersionService,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    output_table_details: Optional[TableDetails] = None,
    request_timestamp: Optional[datetime] = None,
    concatenate_serving_names: Optional[list[str]] = None,
    on_demand_tile_tables: Optional[List[OnDemandTileTable]] = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    Get online features

    Parameters
    ----------
    session_handler: SessionHandler
        SessionHandler to use for executing the query
    cron_helper: CronHelper
        Cron helper for simulating feature job schedules
    column_statistics_service: ColumnStatisticsService
        Column statistics service to get column statistics information
    graph: QueryGraph
        Query graph
    nodes: list[Node]
        List of query graph nodes
    request_data: Union[pd.DataFrame, BatchRequestTableModel]
        Request data as a dataframe or a BatchRequestTableModel
    source_info: SourceInfo
        Source information
    online_store_table_version_service: OnlineStoreTableVersionService
        Online store table version service
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    output_table_details: Optional[TableDetails]
        Optional output table details to write the results to. If this parameter is provided, the
        function will return None (intended to be used when handling asynchronous batch online feature requests).
    request_timestamp: Optional[datetime]
        Request timestamp to use if provided
    concatenate_serving_names: Optional[list[str]]
        List of serving names to concatenate as a new column, if specified
    on_demand_tile_tables: Optional[List[OnDemandTileTable]]
        List of on-demand tile tables to be used in the query

    Returns
    -------
    Optional[List[Dict[str, Any]]]
    """
    tic = time.time()
    session = session_handler.session

    if isinstance(request_data, pd.DataFrame):
        request_table_columns = request_data.columns.tolist()
        request_data[InternalName.TABLE_ROW_INDEX] = range(request_data.shape[0])
        request_table_expr = construct_dataframe_sql_expr(request_data, date_cols=[])
        request_table_details = None
    else:
        request_table_expr = None
        if isinstance(request_data, BatchRequestTableModel):
            request_table_details = request_data.location.table_details
            request_table_columns = [col.name for col in request_data.columns_info]
        else:
            request_table_details = request_data.table_details
            request_table_columns = request_data.column_names[:]

    # Point in time should be provided via request_timestamp. We need to make sure
    # request_table_columns does not contain POINT_IN_TIME since that will be added internally
    request_table_columns = [
        column_name
        for column_name in request_table_columns
        if column_name != SpecialColumnName.POINT_IN_TIME
    ]

    # If using multiple queries, FeatureQuerySet requires request table to be registered as a
    # table beforehand.
    if isinstance(request_data, pd.DataFrame):
        request_table_name = f"{REQUEST_TABLE_NAME}_{session.generate_session_unique_id()}"
        await session.register_table(request_table_name, request_data)
    else:
        assert request_table_details is not None
        request_table_name = request_table_details.table_name

    # Determine the request timestamp to use
    if request_timestamp is None:
        request_timestamp = datetime.utcnow()

    # Register job schedule tables if necessary
    cron_feature_job_settings = get_cron_feature_job_settings(graph, nodes)
    job_schedule_table_set = await cron_helper.register_job_schedule_tables(
        session=session,
        request_timestamp=request_timestamp,
        cron_feature_job_settings=cron_feature_job_settings,
    )

    # Get column statistics information
    column_statistics_info = await column_statistics_service.get_column_statistics_info()

    partition_column_filters = get_partition_filters_from_graph(
        query_graph=graph,
        min_point_in_time=request_timestamp,
        max_point_in_time=request_timestamp,
    )

    try:
        aggregation_result_names = get_aggregation_result_names(graph, nodes, source_info)
        versions = await online_store_table_version_service.get_versions(aggregation_result_names)
        query_set = get_online_features_query_set(
            graph,
            nodes,
            versions=versions,
            source_info=source_info,
            request_table_columns=request_table_columns,
            output_feature_names=get_feature_names(graph, nodes),
            request_table_name=request_table_name,
            request_timestamp=request_timestamp,
            request_table_expr=request_table_expr,
            request_table_details=request_table_details,
            parent_serving_preparation=parent_serving_preparation,
            output_table_details=output_table_details,
            output_include_row_index=request_table_details is None,
            concatenate_serving_names=concatenate_serving_names,
            job_schedule_table_set=job_schedule_table_set,
            on_demand_tile_tables=on_demand_tile_tables,
            column_statistics_info=column_statistics_info,
            partition_column_filters=partition_column_filters,
        )
        logger.debug(f"OnlineServingService sql prep elapsed: {time.time() - tic:.6f}s")

        tic = time.time()
        feature_query_set_result = await execute_feature_query_set(
            session_handler,
            query_set,
            batch_request_table_id=(
                request_data.id if isinstance(request_data, BatchRequestTableModel) else None
            ),
        )
        df_features = feature_query_set_result.dataframe
    finally:
        if request_table_name is not None and request_table_details is None:
            await session.drop_table(
                table_name=request_table_name,
                schema_name=session.schema_name,
                database_name=session.database_name,
            )
        if job_schedule_table_set:
            for job_schedule_table in job_schedule_table_set.tables:
                await session.drop_table(
                    table_name=job_schedule_table.table_name,
                    schema_name=session.schema_name,
                    database_name=session.database_name,
                )

    if output_table_details is None:
        assert df_features is not None
        assert isinstance(request_data, pd.DataFrame)
        df_features = df_features.sort_values(InternalName.TABLE_ROW_INDEX).drop(
            InternalName.TABLE_ROW_INDEX, axis=1
        )
        df_features.index = request_data.index
        features = []
        prepare_dataframe_for_json(df_features)
        for _, row in df_features.iterrows():
            features.append(row.to_dict())
        logger.debug(f"OnlineServingService sql execution elapsed: {time.time() - tic:.6f}s")
        return features

    logger.debug(f"OnlineServingService sql execution elapsed: {time.time() - tic:.6f}s")
    return None
