"""
Module with utility functions to compute historical features
"""

from __future__ import annotations

from typing import Any, Callable, Coroutine, Optional, Union

import time

import pandas as pd
from bson import ObjectId

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.parent_serving import ParentServingPreparation
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
from featurebyte.session.base import BaseSession
from featurebyte.session.session_helper import execute_feature_query_set

logger = get_logger(__name__)


async def compute_tiles_on_demand(  # pylint: disable=too-many-arguments
    session: BaseSession,
    tile_cache_service: TileCacheService,
    graph: QueryGraph,
    nodes: list[Node],
    request_id: str,
    request_table_name: str,
    request_table_columns: list[str],
    feature_store_id: ObjectId,
    serving_names_mapping: Optional[dict[str, str]],
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
) -> None:
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
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
        Optional progress callback function
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
        await tile_cache_service.compute_tiles_on_demand(
            session=session,
            graph=graph,
            nodes=nodes,
            request_id=request_id,
            request_table_name=effective_request_table_name,
            feature_store_id=feature_store_id,
            serving_names_mapping=serving_names_mapping,
            progress_callback=progress_callback,
        )
    finally:
        if parent_serving_preparation is not None:
            await session.drop_table(
                table_name=effective_request_table_name,
                schema_name=session.schema_name,
                database_name=session.database_name,
            )


async def get_historical_features(  # pylint: disable=too-many-locals, too-many-arguments
    session: BaseSession,
    tile_cache_service: TileCacheService,
    graph: QueryGraph,
    nodes: list[Node],
    observation_set: Union[pd.DataFrame, ObservationTableModel],
    feature_store: FeatureStoreModel,
    output_table_details: TableDetails,
    serving_names_mapping: dict[str, str] | None = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
) -> None:
    """Get historical features

    Parameters
    ----------
    session: BaseSession
        Session to use to make queries
    tile_cache_service: TileCacheService
        Tile cache service
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
    """
    tic_ = time.time()

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
        await compute_tiles_on_demand(
            session=session,
            tile_cache_service=tile_cache_service,
            graph=graph,
            nodes=nodes,
            request_id=request_id,
            request_table_name=request_table_name,
            request_table_columns=request_table_columns,
            feature_store_id=feature_store.id,
            serving_names_mapping=serving_names_mapping,
            parent_serving_preparation=parent_serving_preparation,
            progress_callback=(
                tile_cache_progress_callback if tile_cache_progress_callback else None
            ),
        )

        elapsed = time.time() - tic
        logger.debug("Done checking and computing tiles on demand", extra={"duration": elapsed})

        if progress_callback:
            await progress_callback(
                TILE_COMPUTE_PROGRESS_MAX_PERCENT, PROGRESS_MESSAGE_COMPUTING_FEATURES
            )

        # Generate SQL code that computes the features
        historical_feature_query_set = get_historical_features_query_set(
            graph=graph,
            nodes=nodes,
            request_table_columns=request_table_columns,
            serving_names_mapping=serving_names_mapping,
            source_type=feature_store.type,
            output_table_details=output_table_details,
            output_feature_names=get_feature_names(graph, nodes),
            request_table_name=request_table_name,
            parent_serving_preparation=parent_serving_preparation,
            output_include_row_index=output_include_row_index,
            progress_message=PROGRESS_MESSAGE_COMPUTING_FEATURES,
        )
        await execute_feature_query_set(
            session,
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
        logger.debug(f"compute_historical_features in total took {time.time() - tic_:.2f}s")
    finally:
        await session.drop_table(
            table_name=request_table_name,
            schema_name=session.schema_name,
            database_name=session.database_name,
            if_exists=True,
        )


async def get_target(
    session: BaseSession,
    graph: QueryGraph,
    nodes: list[Node],
    observation_set: Union[pd.DataFrame, ObservationTableModel],
    feature_store: FeatureStoreModel,
    output_table_details: TableDetails,
    serving_names_mapping: dict[str, str] | None = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
) -> None:
    """Get target

    Parameters
    ----------
    session: BaseSession
        Session to use to make queries
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
    """
    tic_ = time.time()

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
            source_type=feature_store.type,
            output_table_details=output_table_details,
            output_feature_names=get_feature_names(graph, nodes),
            request_table_name=request_table_name,
            parent_serving_preparation=parent_serving_preparation,
            output_include_row_index=output_include_row_index,
            progress_message=PROGRESS_MESSAGE_COMPUTING_TARGET,
        )

        await execute_feature_query_set(
            session=session,
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
        logger.debug(f"compute_targets in total took {time.time() - tic_:.2f}s")
    finally:
        await session.drop_table(
            table_name=request_table_name,
            schema_name=session.schema_name,
            database_name=session.database_name,
            if_exists=True,
        )
