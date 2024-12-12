"""
Module with utility functions to compute historical features
"""

from __future__ import annotations

import time
from typing import Any, Callable, Coroutine, Optional, Union

import pandas as pd
from bson import ObjectId
from redis import Redis

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.models.system_metrics import HistoricalFeaturesMetrics
from featurebyte.models.tile import OnDemandTileComputeResult
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.batch_helper import NUM_FEATURES_PER_QUERY, get_feature_names
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME
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
from featurebyte.service.tile_cache import TileCacheService
from featurebyte.service.warehouse_table_service import WarehouseTableService
from featurebyte.session.base import BaseSession
from featurebyte.session.session_helper import SessionHandler, execute_feature_query_set

logger = get_logger(__name__)


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
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
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
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
        Optional progress callback function

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
            progress_callback=progress_callback,
        )
    finally:
        logger.info("Cleaning up tables in historical_features_and_target.compute_tiles_on_demand")
        if parent_serving_preparation is not None:
            logger.info("Dropping parent serving table")
            await session.drop_table(
                table_name=effective_request_table_name,
                schema_name=session.schema_name,
                database_name=session.database_name,
                if_exists=True,
            )
            logger.info("Done dropping parent serving table")
    return tile_compute_result


async def get_historical_features(
    session: BaseSession,
    tile_cache_service: TileCacheService,
    warehouse_table_service: WarehouseTableService,
    graph: QueryGraph,
    nodes: list[Node],
    observation_set: Union[pd.DataFrame, ObservationTableModel],
    feature_store: FeatureStoreModel,
    output_table_details: TableDetails,
    serving_names_mapping: dict[str, str] | None = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
) -> HistoricalFeaturesMetrics:
    """Get historical features

    Parameters
    ----------
    session: BaseSession
        Session to use to make queries
    tile_cache_service: TileCacheService
        Tile cache service
    warehouse_table_service: WarehouseTableService
        Warehouse table service
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
    validate_historical_requests_point_in_time(observation_set)

    # use a unique request table name
    request_id = session.generate_session_unique_id()
    request_table_name = f"{REQUEST_TABLE_NAME}_{request_id}"
    request_table_columns = observation_set.columns

    # Execute feature SQL code
    await observation_set.register_as_request_table(
        session, request_table_name, add_row_index=len(nodes) > NUM_FEATURES_PER_QUERY
    )

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
            parent_serving_preparation=parent_serving_preparation,
            progress_callback=(
                tile_cache_progress_callback if tile_cache_progress_callback else None
            ),
        )

        tile_compute_seconds = time.time() - tic
        logger.debug(
            "Done checking and computing tiles on demand", extra={"duration": tile_compute_seconds}
        )

        if progress_callback:
            await progress_callback(
                TILE_COMPUTE_PROGRESS_MAX_PERCENT, PROGRESS_MESSAGE_COMPUTING_FEATURES
            )

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
            output_include_row_index=output_include_row_index,
            progress_message=PROGRESS_MESSAGE_COMPUTING_FEATURES,
        )
        await execute_feature_query_set(
            session_handler=SessionHandler(
                session=session,
                redis=tile_cache_service.tile_manager_service.redis,
                feature_store=feature_store,
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
        )
        feature_compute_seconds = time.time() - tic
        logger.debug(f"compute_historical_features in total took {feature_compute_seconds:.2f}s")
    finally:
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

    return HistoricalFeaturesMetrics(
        tile_compute_seconds=tile_compute_seconds,
        tile_compute_metrics=tile_compute_result.tile_compute_metrics,
        feature_compute_seconds=feature_compute_seconds,
    )


async def get_target(
    session: BaseSession,
    redis: Redis[Any],
    graph: QueryGraph,
    nodes: list[Node],
    observation_set: Union[pd.DataFrame, ObservationTableModel],
    feature_store: FeatureStoreModel,
    output_table_details: TableDetails,
    serving_names_mapping: dict[str, str] | None = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
) -> HistoricalFeaturesMetrics:
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
        add_row_index=len(nodes) > NUM_FEATURES_PER_QUERY,
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
                session=session, redis=redis, feature_store=feature_store
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
        )
        feature_compute_seconds = time.time() - tic
        logger.debug(f"compute_targets in total took {feature_compute_seconds:.2f}s")
    finally:
        await session.drop_table(
            table_name=request_table_name,
            schema_name=session.schema_name,
            database_name=session.database_name,
            if_exists=True,
        )
    return HistoricalFeaturesMetrics(
        tile_compute_seconds=0,
        feature_compute_seconds=feature_compute_seconds,
    )
