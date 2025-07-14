"""
Module with utility functions to compute historical features
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional, Tuple, Union

import pandas as pd
from bson import ObjectId
from dateutil import parser as dateutil_parser
from redis import Redis

from featurebyte.common.env_util import is_feature_query_debug_enabled
from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.models.system_metrics import HistoricalFeaturesMetrics
from featurebyte.models.tile import OnDemandTileComputeResult
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupByNode
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.batch_helper import get_feature_names
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME, PartitionColumnFilters
from featurebyte.query_graph.sql.cron import JobScheduleTableSet, get_cron_feature_job_settings
from featurebyte.query_graph.sql.feature_historical import (
    PROGRESS_MESSAGE_COMPUTING_FEATURES,
    PROGRESS_MESSAGE_COMPUTING_TARGET,
    TILE_COMPUTE_PROGRESS_MAX_PERCENT,
    get_historical_features_query_set,
    get_internal_observation_set,
    validate_historical_requests_point_in_time,
    validate_request_schema,
)
from featurebyte.query_graph.sql.parent_serving import construct_request_table_with_parent_entities
from featurebyte.query_graph.sql.partition_filter_helper import get_partition_filters_from_graph
from featurebyte.service.column_statistics import ColumnStatisticsService
from featurebyte.service.cron_helper import CronHelper
from featurebyte.service.system_metrics import SystemMetricsService
from featurebyte.service.tile_cache import TileCacheService
from featurebyte.service.warehouse_table_service import WarehouseTableService
from featurebyte.session.base import BaseSession
from featurebyte.session.session_helper import SessionHandler, execute_feature_query_set

logger = get_logger(__name__)


@dataclass
class FeaturesComputationResult:
    """
    Features computation result
    """

    historical_features_metrics: HistoricalFeaturesMetrics
    failed_node_names: list[str] = field(default_factory=list)


async def compute_tiles_on_demand(
    session: BaseSession,
    tile_cache_service: TileCacheService,
    graph: QueryGraph,
    nodes: list[Node],
    request_id: str,
    request_table_name: str,
    request_table_columns: list[str],
    feature_store_id: ObjectId,
    serving_names_mapping: Optional[dict[str, str]],
    temp_tile_tables_tag: str,
    partition_column_filters: Optional[PartitionColumnFilters],
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    raise_on_error: bool = True,
) -> OnDemandTileComputeResult:
    """
    Compute tiles on demand

    Parameters
    ----------
    session: BaseSession
        Session to use to make queries
    tile_cache_service: TileCacheService
        Tile cache service
    graph: QueryGraph
        Query graph
    nodes: list[Node]
        List of query graph node
    request_id: str
        Request ID to be used as suffix of table names when creating temporary tables
    request_table_name: str
        Name of request table
    feature_store_id: ObjectId
        Feature store id
    request_table_columns: list[str]
        List of column names in the observations set
    serving_names_mapping : dict[str, str] | None
        Optional serving names mapping if the training events data has different serving name
        columns than those defined in Entities
    temp_tile_tables_tag: str
        Tag to identify the temporary tile tables for cleanup purpose
    partition_column_filters: Optional[PartitionColumnFilters]
        Optional partition column filters to apply
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
        Optional progress callback function
    raise_on_error: bool
        Whether to raise an error if the computation fails

    Returns
    -------
    TileComputeMetrics
    """
    if parent_serving_preparation is None:
        effective_request_table_name = request_table_name
    else:
        # Lookup parent entities and join them with the request table since tile computation
        # requires these entity columns to be present in the request table.
        parent_serving_result = construct_request_table_with_parent_entities(
            request_table_name=request_table_name,
            request_table_columns=request_table_columns,
            join_steps=parent_serving_preparation.join_steps,
            feature_store_details=parent_serving_preparation.feature_store_details,
        )
        effective_request_table_name = parent_serving_result.new_request_table_name
        await session.create_table_as(
            TableDetails(table_name=effective_request_table_name),
            parent_serving_result.table_expr,
        )

    try:
        tile_compute_result = await tile_cache_service.compute_tiles_on_demand(
            session=session,
            graph=graph,
            nodes=nodes,
            request_id=request_id,
            request_table_name=effective_request_table_name,
            feature_store_id=feature_store_id,
            serving_names_mapping=serving_names_mapping,
            temp_tile_tables_tag=temp_tile_tables_tag,
            partition_column_filters=partition_column_filters,
            progress_callback=progress_callback,
            raise_on_error=raise_on_error,
        )
    finally:
        logger.info("Cleaning up tables in historical_features_and_target.compute_tiles_on_demand")
        if parent_serving_preparation is not None and not is_feature_query_debug_enabled():
            logger.info("Dropping parent serving table")
            await session.drop_table(
                table_name=effective_request_table_name,
                schema_name=session.schema_name,
                database_name=session.database_name,
                if_exists=True,
            )
            logger.info("Done dropping parent serving table")
    return tile_compute_result


def filter_failed_tile_nodes(
    graph: QueryGraph, nodes: list[Node], failed_tile_table_ids: list[str]
) -> Tuple[list[Node], list[Node]]:
    """
    Filter out nodes that are associated with failed tile tables

    Parameters
    ----------
    graph: QueryGraph
        Query graph
    nodes: list[Node]
        List of query graph node
    failed_tile_table_ids: list[str]
        List of failed tile table ids

    Returns
    -------
    Tuple[list[Node], list[Node]]
        Filtered nodes and failed tile nodes
    """
    failed_tile_table_ids_set = set(failed_tile_table_ids)
    filtered_nodes = []
    failed_tile_nodes = []
    for node in nodes:
        is_failed_tile_node = False
        for groupby_node in graph.iterate_nodes(node, NodeType.GROUPBY):
            assert isinstance(groupby_node, GroupByNode)
            if groupby_node.parameters.tile_id in failed_tile_table_ids_set:
                is_failed_tile_node = True
                break
        if is_failed_tile_node:
            failed_tile_nodes.append(node)
        else:
            filtered_nodes.append(node)
    return filtered_nodes, failed_tile_nodes


async def get_historical_features(
    session: BaseSession,
    tile_cache_service: TileCacheService,
    warehouse_table_service: WarehouseTableService,
    cron_helper: CronHelper,
    column_statistics_service: ColumnStatisticsService,
    system_metrics_service: SystemMetricsService,
    graph: QueryGraph,
    nodes: list[Node],
    observation_set: Union[pd.DataFrame, ObservationTableModel],
    feature_store: FeatureStoreModel,
    output_table_details: TableDetails,
    serving_names_mapping: dict[str, str] | None = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    raise_on_error: bool = True,
) -> FeaturesComputationResult:
    """Get historical features

    Parameters
    ----------
    session: BaseSession
        Session to use to make queries
    tile_cache_service: TileCacheService
        Tile cache service
    cron_helper: CronHelper
        Cron helper for simulating feature job schedules
    warehouse_table_service: WarehouseTableService
        Warehouse table service
    column_statistics_service: ColumnStatisticsService
        Column statistics service
    system_metrics_service: SystemMetricsService
        System metrics service
    graph : QueryGraph
        Query graph
    nodes : list[Node]
        List of query graph node
    observation_set : Union[pd.DataFrame, ObservationTableModel]
        Observation set
    feature_store: FeatureStoreModel
        Feature store. We need the feature store id and source type information.
    serving_names_mapping : dict[str, str] | None
        Optional serving names mapping if the observations set has different serving name columns
        than those defined in Entities
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    output_table_details: TableDetails
        Output table details to write the results to
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
        Optional progress callback function
    raise_on_error: bool
        Whether to raise an error if the computation fails. If True, any query error will be raised
        immediately. If False, computation will be done on a best effort basis with errors
        suppressed and logged in the result.

    Returns
    -------
    HistoricalFeaturesMetrics
    """
    if isinstance(observation_set, ObservationTableModel):
        observation_table_model = observation_set
    else:
        observation_table_model = None
    output_include_row_index = (
        observation_table_model is not None and observation_table_model.has_row_index is True
    )
    observation_table_id = observation_table_model.id if observation_table_model else None
    observation_set = get_internal_observation_set(observation_set)

    # Validate request
    validate_request_schema(observation_set)
    validate_historical_requests_point_in_time(observation_set)

    # use a unique request table name
    request_id = session.generate_session_unique_id()
    request_table_name = f"{REQUEST_TABLE_NAME}_{request_id}"
    request_table_columns = observation_set.columns

    # Execute feature SQL code
    await observation_set.register_as_request_table(session, request_table_name, add_row_index=True)

    # Register job schedule tables if necessary
    cron_feature_job_settings = get_cron_feature_job_settings(graph, nodes)
    job_schedule_table_set = await cron_helper.register_job_schedule_tables(
        session=session,
        request_table_name=request_table_name,
        cron_feature_job_settings=cron_feature_job_settings,
    )

    # Get column statistics info
    column_statistics_info = await column_statistics_service.get_column_statistics_info()

    # Get partition column filters if applicable (requires information from the observation table)
    if (
        observation_table_model is not None
        and observation_table_model.least_recent_point_in_time is not None
    ):
        min_point_in_time = dateutil_parser.parse(
            observation_table_model.least_recent_point_in_time
        )
        max_point_in_time = dateutil_parser.parse(observation_table_model.most_recent_point_in_time)
        partition_column_filters = get_partition_filters_from_graph(
            graph, min_point_in_time, max_point_in_time
        )
    else:
        partition_column_filters = None

    temp_tile_tables_tag = f"historical_features_{output_table_details.table_name}"
    try:
        # Compute tiles on demand if required
        tile_cache_progress_callback = (
            get_ranged_progress_callback(
                progress_callback,
                0,
                TILE_COMPUTE_PROGRESS_MAX_PERCENT,
            )
            if progress_callback
            else None
        )
        tic = time.time()
        tile_compute_result = await compute_tiles_on_demand(
            session=session,
            tile_cache_service=tile_cache_service,
            graph=graph,
            nodes=nodes,
            request_id=request_id,
            request_table_name=request_table_name,
            request_table_columns=request_table_columns,
            feature_store_id=feature_store.id,
            serving_names_mapping=serving_names_mapping,
            temp_tile_tables_tag=temp_tile_tables_tag,
            partition_column_filters=partition_column_filters,
            parent_serving_preparation=parent_serving_preparation,
            progress_callback=(
                tile_cache_progress_callback if tile_cache_progress_callback else None
            ),
            raise_on_error=raise_on_error,
        )

        tile_compute_seconds = time.time() - tic
        logger.debug(
            "Done checking and computing tiles on demand", extra={"duration": tile_compute_seconds}
        )

        if progress_callback:
            await progress_callback(
                TILE_COMPUTE_PROGRESS_MAX_PERCENT, PROGRESS_MESSAGE_COMPUTING_FEATURES
            )

        # Skip computing features associated with failed tile tables since they are bound to fail
        if tile_compute_result.failed_tile_table_ids:
            logger.warning(
                "Some tiles failed to compute: %s",
                tile_compute_result.failed_tile_table_ids,
            )
            nodes, failed_tile_nodes = filter_failed_tile_nodes(
                graph, nodes, tile_compute_result.failed_tile_table_ids
            )
            failed_node_names = [node.name for node in failed_tile_nodes]
        else:
            failed_node_names = []

        # Generate SQL code that computes the features
        tic = time.time()
        historical_feature_query_set = get_historical_features_query_set(
            graph=graph,
            nodes=nodes,
            request_table_columns=request_table_columns,
            serving_names_mapping=serving_names_mapping,
            source_info=feature_store.get_source_info(),
            output_table_details=output_table_details,
            output_feature_names=get_feature_names(graph, nodes),
            request_table_name=request_table_name,
            parent_serving_preparation=parent_serving_preparation,
            on_demand_tile_tables=tile_compute_result.on_demand_tile_tables,
            job_schedule_table_set=job_schedule_table_set,
            column_statistics_info=column_statistics_info,
            partition_column_filters=partition_column_filters,
            output_include_row_index=output_include_row_index,
            progress_message=PROGRESS_MESSAGE_COMPUTING_FEATURES,
        )
        feature_query_set_result = await execute_feature_query_set(
            session_handler=SessionHandler(
                session=session,
                redis=tile_cache_service.tile_manager_service.redis,
                feature_store=feature_store,
                system_metrics_service=system_metrics_service,
            ),
            feature_query_set=historical_feature_query_set,
            progress_callback=(
                get_ranged_progress_callback(
                    progress_callback,
                    TILE_COMPUTE_PROGRESS_MAX_PERCENT,
                    100,
                )
                if progress_callback
                else None
            ),
            raise_on_error=raise_on_error,
            observation_table_id=observation_table_id,
        )
        feature_compute_seconds = time.time() - tic
        logger.debug(f"compute_historical_features in total took {feature_compute_seconds:.2f}s")
    finally:
        if not is_feature_query_debug_enabled():
            await cleanup_historical_features_temp_tables(
                session=session,
                feature_store=feature_store,
                warehouse_table_service=warehouse_table_service,
                request_table_name=request_table_name,
                temp_tile_tables_tag=temp_tile_tables_tag,
                job_schedule_table_set=job_schedule_table_set,
            )

    failed_node_names.extend(feature_query_set_result.failed_node_names)
    metrics = HistoricalFeaturesMetrics(
        tile_compute_seconds=tile_compute_seconds,
        tile_compute_metrics=tile_compute_result.tile_compute_metrics,
        feature_compute_seconds=feature_compute_seconds,
    )
    return FeaturesComputationResult(
        historical_features_metrics=metrics,
        failed_node_names=failed_node_names,
    )


async def cleanup_historical_features_temp_tables(
    session: BaseSession,
    feature_store: FeatureStoreModel,
    warehouse_table_service: WarehouseTableService,
    request_table_name: str,
    temp_tile_tables_tag: str,
    job_schedule_table_set: JobScheduleTableSet,
) -> None:
    """
    Cleanup historical features temporary tables

    Parameters
    ----------
    session: BaseSession
        Session to use to make queries
    feature_store: FeatureStoreModel
        Feature store
    warehouse_table_service: WarehouseTableService
        Warehouse table service
    request_table_name: str
        Name of request table
    temp_tile_tables_tag: str
        Tag to identify the temporary tile tables for cleanup purpose
    job_schedule_table_set: JobScheduleTableSet
        Job schedule table set
    """
    logger.info("Cleaning up tables in get_historical_features")
    await session.drop_table(
        table_name=request_table_name,
        schema_name=session.schema_name,
        database_name=session.database_name,
        if_exists=True,
    )
    async for warehouse_table in warehouse_table_service.list_warehouse_tables_by_tag(
        temp_tile_tables_tag
    ):
        await warehouse_table_service.drop_table_with_session(
            session=session,
            feature_store_id=feature_store.id,
            table_name=warehouse_table.location.table_details.table_name,
            schema_name=warehouse_table.location.table_details.schema_name,
            database_name=warehouse_table.location.table_details.database_name,
            if_exists=True,
        )
    await session.drop_tables(
        table_names=[
            job_schedule_table.table_name for job_schedule_table in job_schedule_table_set.tables
        ],
        schema_name=session.schema_name,
        database_name=session.database_name,
        if_exists=True,
    )


async def get_target(
    session: BaseSession,
    redis: Redis[Any],
    graph: QueryGraph,
    nodes: list[Node],
    observation_set: Union[pd.DataFrame, ObservationTableModel],
    feature_store: FeatureStoreModel,
    output_table_details: TableDetails,
    system_metrics_service: SystemMetricsService,
    serving_names_mapping: dict[str, str] | None = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
) -> FeaturesComputationResult:
    """Get target

    Parameters
    ----------
    session: BaseSession
        Session to use to make queries
    redis: Redis[Any]
        Redis connection
    graph : QueryGraph
        Query graph
    nodes : list[Node]
        List of query graph node
    observation_set : Union[pd.DataFrame, ObservationTableModel]
        Observation set
    feature_store: FeatureStoreModel
        Feature store. We need the feature store id and source type information.
    output_table_details: TableDetails
        Output table details to write the results to
    system_metrics_service: SystemMetricsService
        System metrics service
    serving_names_mapping : dict[str, str] | None
        Optional serving names mapping if the observations set has different serving name columns
        than those defined in Entities
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
        Optional progress callback function

    Returns
    -------
    HistoricalFeaturesMetrics
    """
    output_include_row_index = (
        isinstance(observation_set, ObservationTableModel) and observation_set.has_row_index is True
    )

    observation_set = get_internal_observation_set(observation_set)

    # Validate request
    validate_request_schema(observation_set)

    # use a unique request table name
    request_id = session.generate_session_unique_id()
    request_table_name = f"{REQUEST_TABLE_NAME}_{request_id}"
    request_table_columns = observation_set.columns

    # Execute feature SQL code
    await observation_set.register_as_request_table(
        session,
        request_table_name,
        add_row_index=True,
    )

    # Generate SQL code that computes the targets
    try:
        historical_feature_query_set = get_historical_features_query_set(
            graph=graph,
            nodes=nodes,
            request_table_columns=request_table_columns,
            serving_names_mapping=serving_names_mapping,
            source_info=feature_store.get_source_info(),
            output_table_details=output_table_details,
            output_feature_names=get_feature_names(graph, nodes),
            request_table_name=request_table_name,
            parent_serving_preparation=parent_serving_preparation,
            output_include_row_index=output_include_row_index,
            progress_message=PROGRESS_MESSAGE_COMPUTING_TARGET,
        )

        tic = time.time()
        await execute_feature_query_set(
            session_handler=SessionHandler(
                session=session,
                redis=redis,
                feature_store=feature_store,
                system_metrics_service=system_metrics_service,
            ),
            feature_query_set=historical_feature_query_set,
            progress_callback=(
                get_ranged_progress_callback(
                    progress_callback,
                    TILE_COMPUTE_PROGRESS_MAX_PERCENT,
                    100,
                )
                if progress_callback
                else None
            ),
            observation_table_id=(
                observation_set.id if isinstance(observation_set, ObservationTableModel) else None
            ),
        )
        feature_compute_seconds = time.time() - tic
        logger.debug(f"compute_targets in total took {feature_compute_seconds:.2f}s")
    finally:
        if not is_feature_query_debug_enabled():
            await session.drop_table(
                table_name=request_table_name,
                schema_name=session.schema_name,
                database_name=session.database_name,
                if_exists=True,
            )
    return FeaturesComputationResult(
        historical_features_metrics=HistoricalFeaturesMetrics(
            tile_compute_seconds=0,
            feature_compute_seconds=feature_compute_seconds,
        )
    )
