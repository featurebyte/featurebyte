"""
Session related helper functions
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, List, Optional

import pandas as pd
from bson import ObjectId
from redis import Redis
from sqlglot import expressions

from featurebyte.common.env_util import is_feature_query_debug_enabled
from featurebyte.common.progress import divide_progress_callback
from featurebyte.common.utils import timer
from featurebyte.enum import InternalName
from featurebyte.exception import FeatureQueryExecutionError, InvalidOutputRowIndexError
from featurebyte.logging import get_logger
from featurebyte.models import FeatureStoreModel
from featurebyte.models.feature_query_set import FeatureQuerySet
from featurebyte.models.system_metrics import SqlQueryMetrics, SqlQueryType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.batch_helper import (
    NUM_FEATURES_PER_QUERY,
)
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.feature_compute import FeatureQuery
from featurebyte.service.system_metrics import SystemMetricsService
from featurebyte.session.base import BaseSession, QueryMetadata
from featurebyte.utils.async_helper import asyncio_gather

logger = get_logger(__name__)


MAX_QUERY_CONCURRENCY = int(os.getenv("MAX_QUERY_CONCURRENCY", "3"))
SQL_QUERY_METRICS_LOGGING_THRESHOLD_SECONDS = int(
    os.getenv("FEATUREBYTE_SQL_QUERY_METRICS_LOGGING_THRESHOLD_SECONDS", "600")
)


async def validate_output_row_index(session: BaseSession, output_table_name: str) -> None:
    """
    Validate row index is unique in the generated output table

    Parameters
    ----------
    session: BaseSession
        Database session
    output_table_name: str
        Name of the table to be validated

    Raises
    ------
    InvalidOutputRowIndexError
        If any of the row index value is not unique
    """
    query = sql_to_string(
        expressions.select(
            expressions.alias_(
                expressions.EQ(
                    this=expressions.Count(
                        this=expressions.Distinct(
                            expressions=[quoted_identifier(InternalName.TABLE_ROW_INDEX.value)]
                        ),
                    ),
                    expression=expressions.Count(this=expressions.Star()),
                ),
                alias="is_row_index_valid",
                quoted=True,
            )
        ).from_(quoted_identifier(output_table_name)),
        source_type=session.source_type,
    )
    with timer("Validate output row index", logger=logger):
        df_result = await session.execute_query_long_running(query)
    is_row_index_valid = df_result["is_row_index_valid"].iloc[0]  # type: ignore[index]
    if not is_row_index_valid:
        raise InvalidOutputRowIndexError("Row index column is invalid in the output table")


async def run_coroutines(
    coroutines: List[Coroutine[Any, Any, Any]],
    redis: Redis[Any],
    concurrency_key: str,
    max_concurrency: Optional[int],
    return_exceptions: bool = False,
) -> List[Any]:
    """
    Execute the provided list of coroutines

    Parameters
    ----------
    coroutines: List[Coroutine[Any, Any, None]]
        List of coroutines to be executed
    redis: Redis[Any]
        Redis connection
    concurrency_key: str
        Key for concurrency limit enforcement
    max_concurrency: Optional[int]
        Maximum number of coroutines to run concurrently or None to use the default
    return_exceptions: bool
        When True, exceptions are gathered in the result list instead of being raised immediately
        and will not trigger cancellation of other tasks.

    Returns
    -------
    List[Any]
        List of results from the coroutines
    """
    max_concurrency = max_concurrency or MAX_QUERY_CONCURRENCY
    future = asyncio_gather(
        *coroutines,
        redis=redis,
        concurrency_key=concurrency_key,
        max_concurrency=max_concurrency,
        return_exceptions=return_exceptions,
    )
    return await future


async def execute_feature_query(
    session: BaseSession,
    feature_query: FeatureQuery,
    done_callback: Callable[[int], Coroutine[Any, Any, None]],
    system_metrics_service: SystemMetricsService,
    observation_table_id: Optional[ObjectId] = None,
    batch_request_table_id: Optional[ObjectId] = None,
) -> FeatureQuery:
    """
    Process a single FeatureQuery

    Parameters
    ----------
    session: BaseSession
        Session object
    feature_query: FeatureQuery
        Instance of a FeatureQuery
    done_callback: Optional[Callable[[int], Coroutine[Any, Any, None]]]
        To be called when task is completed to update progress
    system_metrics_service: SystemMetricsService
        System metrics service
    observation_table_id: Optional[ObjectId]
        ID of the observation table, if applicable
    batch_request_table_id: Optional[ObjectId]
        ID of the batch request table, if applicable

    Returns
    -------
    FeatureQuery

    Raises
    ------
    InvalidOutputRowIndexError
        If the row index column is not unique in the intermediate feature table
    """
    session = await session.clone_if_not_threadsafe()

    materialized_temp_tables = []
    try:
        for temp_table_query in feature_query.temp_table_queries:
            await session.execute_query_long_running(temp_table_query.sql)
            materialized_temp_tables.append(temp_table_query.table_name)

        feature_sql = feature_query.feature_table_query.sql
        feature_table_name = feature_query.feature_table_query.table_name
        tic = time.time()
        query_metadata = QueryMetadata()
        await session.execute_query_long_running(feature_sql, query_metadata=query_metadata)
        elapsed = time.time() - tic
        if elapsed > SQL_QUERY_METRICS_LOGGING_THRESHOLD_SECONDS:
            await system_metrics_service.create_metrics(
                SqlQueryMetrics(
                    query=feature_sql,
                    total_seconds=elapsed,
                    query_type=SqlQueryType.FEATURE_COMPUTE,
                    query_id=query_metadata.query_id,
                    feature_names=sorted(feature_query.feature_names),
                    observation_table_id=observation_table_id,
                    batch_request_table_id=batch_request_table_id,
                )
            )
        try:
            await validate_output_row_index(session, feature_table_name)
        except InvalidOutputRowIndexError:
            formatted_feature_names = ", ".join(sorted(feature_query.feature_names))
            raise InvalidOutputRowIndexError(
                f"Row index column is invalid in the intermediate feature table: {feature_table_name}."
                f" Feature names: {formatted_feature_names}"
            )
    finally:
        if not is_feature_query_debug_enabled():
            await session.drop_tables(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_names=materialized_temp_tables,
                if_exists=True,
            )

    await done_callback(len(feature_query.node_names))
    return feature_query


class SessionHandler:
    def __init__(
        self,
        session: BaseSession,
        redis: Redis[Any],
        feature_store: FeatureStoreModel,
        system_metrics_service: SystemMetricsService,
    ):
        self.session = session
        self.redis = redis
        self.feature_store = feature_store
        self.system_metrics_service = system_metrics_service


@dataclass
class FeatureQuerySetResult:
    """
    FeatureQuerySetResult is used to store the result of executing a feature query set.
    """

    dataframe: Optional[pd.DataFrame]
    failed_node_names: List[str]


async def execute_feature_query_set(
    session_handler: SessionHandler,
    feature_query_set: FeatureQuerySet,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
    raise_on_error: bool = True,
    observation_table_id: Optional[ObjectId] = None,
    batch_request_table_id: Optional[ObjectId] = None,
) -> FeatureQuerySetResult:
    """
    Execute the feature queries to materialize features

    Parameters
    ----------
    session_handler: SessionHandler
        SessionHandler object
    feature_query_set: FeatureQuerySet
        FeatureQuerySet object
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
        Optional progress callback function
    raise_on_error: bool
        Whether to raise an error if any of the feature queries fail to materialize
    observation_table_id: Optional[ObjectId]
        ID of the observation table, if applicable
    batch_request_table_id: Optional[ObjectId]
        ID of the batch request table, if applicable

    Returns
    -------
    FeatureQuerySetResult

    Raises
    ------
    FeatureQueryExecutionError
        If any of the feature queries fail to materialize after attempts to retry with
        simplification
    """
    session = session_handler.session
    source_info = session.get_source_info()
    total_num_nodes = feature_query_set.get_num_nodes()
    materialized_feature_table: list[str] = []
    processed = 0

    # Allocate 90% of the progress to feature queries and 10% to the final output query
    if progress_callback is None:
        feature_queries_progress_callback, output_query_progress_callback = None, None
    else:
        feature_queries_progress_callback, output_query_progress_callback = (
            divide_progress_callback(progress_callback, 90)
        )

    async def _progress_callback(num_nodes_completed: int) -> None:
        nonlocal processed
        processed += num_nodes_completed
        if feature_queries_progress_callback:
            await feature_queries_progress_callback(
                int(100 * processed / total_num_nodes),
                feature_query_set.progress_message,
            )

    generator = feature_query_set.feature_query_generator
    num_features_per_query = min(total_num_nodes, NUM_FEATURES_PER_QUERY)
    table_name_suffix_counter = 0
    feature_query_results = []
    previous_node_groups: Optional[list[list[Node]]] = None
    try:
        while num_features_per_query >= 1:
            pending_node_names = feature_query_set.get_pending_node_names()
            if not pending_node_names:
                break
            node_groups = generator.split_nodes(
                pending_node_names, num_features_per_query, source_info
            )
            if previous_node_groups is not None and previous_node_groups == node_groups:
                # Exit early if the same node groups are being processed - it means that the feature
                # queries cannot be split further
                break
            feature_query_results = await execute_queries_for_node_groups(
                feature_query_set=feature_query_set,
                node_groups=node_groups,
                session_handler=session_handler,
                materialized_feature_table=materialized_feature_table,
                table_name_suffix_counter=table_name_suffix_counter,
                progress_callback=_progress_callback,
                observation_table_id=observation_table_id,
                batch_request_table_id=batch_request_table_id,
            )
            table_name_suffix_counter += len(node_groups)
            num_features_per_query //= 2
            previous_node_groups = node_groups

        failed_node_names = feature_query_set.get_pending_node_names()
        if failed_node_names:
            failed_feature_names = ", ".join(sorted(generator.get_feature_names(failed_node_names)))
            exception_result = None
            for feature_query_result in feature_query_results:
                if isinstance(feature_query_result, Exception):
                    exception_result = feature_query_result
            assert exception_result is not None
            if raise_on_error:
                raise FeatureQueryExecutionError(
                    f"Failed to materialize {len(failed_node_names)} features: {failed_feature_names}"
                ) from exception_result

        output_query = feature_query_set.construct_output_query(session.get_source_info())
        result = await session.execute_query_long_running(output_query)
        if (
            feature_query_set.output_include_row_index
            and feature_query_set.output_table_name is not None
        ):
            await validate_output_row_index(session, feature_query_set.output_table_name)
        if output_query_progress_callback:
            await output_query_progress_callback(100, feature_query_set.progress_message)
        return FeatureQuerySetResult(dataframe=result, failed_node_names=failed_node_names)

    finally:
        await session.drop_tables(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_names=materialized_feature_table,
            if_exists=True,
        )


async def execute_queries_for_node_groups(
    feature_query_set: FeatureQuerySet,
    node_groups: List[List[Node]],
    session_handler: SessionHandler,
    materialized_feature_table: List[str],
    table_name_suffix_counter: int,
    progress_callback: Callable[[int], Coroutine[Any, Any, None]],
    observation_table_id: Optional[ObjectId] = None,
    batch_request_table_id: Optional[ObjectId] = None,
) -> list[FeatureQuery | Exception]:
    """
    Execute the feature queries for a list of node groups and update feature_query_set accordingly

    Parameters
    ----------
    feature_query_set: FeatureQuerySet
        FeatureQuerySet object
    node_groups: List[List[Node]]
        List of node groups
    session_handler: SessionHandler
        SessionHandler object
    materialized_feature_table: List[str]
        List of materialized feature tables
    table_name_suffix_counter: int
        Counter for table name suffix
    progress_callback: Callable[[int], Coroutine[Any, Any, None]]
        Progress callback function
    observation_table_id: Optional[ObjectId]
        ID of the observation table, if applicable
    batch_request_table_id: Optional[ObjectId]
        ID of the batch request table, if applicable

    Returns
    -------
    list[FeatureQuery | Exception]

    Raises
    ------
    feature_query_result
        The exception raised by execute_feature_query when a feature query fails to materialize
    """
    generator = feature_query_set.feature_query_generator
    feature_set_table_name_prefix = f"__TEMP_{ObjectId()}"

    coroutines = []
    all_node_names = []
    for i, nodes_group in enumerate(node_groups):
        suffix = i + table_name_suffix_counter
        feature_set_table_name = f"{feature_set_table_name_prefix}_{suffix}"
        feature_query = generator.generate_feature_query(
            node_names=[node.name for node in nodes_group],
            table_name=feature_set_table_name,
        )
        coroutines.append(
            execute_feature_query(
                session=session_handler.session,
                feature_query=feature_query,
                done_callback=progress_callback,
                system_metrics_service=session_handler.system_metrics_service,
                observation_table_id=observation_table_id,
                batch_request_table_id=batch_request_table_id,
            )
        )
        materialized_feature_table.append(feature_query.feature_table_query.table_name)
        all_node_names.extend([node.name for node in nodes_group])

    if coroutines:
        with timer("Execute feature queries", logger=logger):
            feature_query_results = await run_coroutines(
                coroutines,
                session_handler.redis,
                str(session_handler.feature_store.id),
                session_handler.feature_store.max_query_concurrency,
                return_exceptions=True,
            )

    completed_node_names = []
    for feature_query_result in feature_query_results:
        if isinstance(feature_query_result, FeatureQuery):
            feature_query_set.add_completed_feature_query(feature_query_result)
            completed_node_names.extend(feature_query_result.node_names)
        elif isinstance(feature_query_result, InvalidOutputRowIndexError):
            # Raise InvalidOutputRowIndexError immediately since that is likely a bug that cannot be
            # recovered from retrying
            raise feature_query_result
        else:
            # If the result is an exception, we will log it and continue
            logger.error(f"Failed to materialize feature query: {feature_query_result}")

    failed_node_names = list(set(all_node_names) - set(completed_node_names))
    if failed_node_names:
        failed_feature_names = ", ".join(generator.get_feature_names(failed_node_names))
        logger.warning(
            f"Failed to materialize {len(failed_feature_names)} features: {failed_feature_names}"
        )

    return feature_query_results
