"""
Session related helper functions
"""

from __future__ import annotations

import os
from typing import Any, Callable, Coroutine, List, Optional, Union

import pandas as pd
from bson import ObjectId
from redis import Redis
from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.common.progress import divide_progress_callback
from featurebyte.common.utils import timer
from featurebyte.enum import InternalName, SourceType
from featurebyte.exception import InvalidOutputRowIndexError
from featurebyte.logging import get_logger
from featurebyte.models import FeatureStoreModel
from featurebyte.models.feature_query_set import FeatureQuerySet
from featurebyte.query_graph.sql.batch_helper import (
    NUM_FEATURES_PER_QUERY,
    FeatureQuery,
)
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.session.base import BaseSession
from featurebyte.utils.async_helper import asyncio_gather

logger = get_logger(__name__)


MAX_QUERY_CONCURRENCY = int(os.getenv("MAX_QUERY_CONCURRENCY", "3"))


def _to_query_str(query: Union[str, Expression], source_type: SourceType) -> str:
    # TODO: still needed?
    if isinstance(query, str):
        return query
    assert isinstance(query, Expression)
    return sql_to_string(query, source_type)


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

    Raises
    ------
    InvalidOutputRowIndexError
        If the row index column is not unique in the intermediate feature table
    """
    session = await session.clone_if_not_threadsafe()
    await session.execute_query_long_running(_to_query_str(feature_query.sql, session.source_type))
    try:
        await validate_output_row_index(session, feature_query.table_name)
    except InvalidOutputRowIndexError:
        formatted_feature_names = ", ".join(feature_query.feature_names)
        raise InvalidOutputRowIndexError(
            f"Row index column is invalid in the intermediate feature table: {feature_query.table_name}."
            f" Feature names: {formatted_feature_names}"
        )

    await done_callback(len(feature_query.node_names))
    return feature_query


class SessionHandler:
    def __init__(self, session: BaseSession, redis: Redis[Any], feature_store: FeatureStoreModel):
        self.session = session
        self.redis = redis
        self.feature_store = feature_store


async def execute_feature_query_set(
    session_handler: SessionHandler,
    feature_query_set: FeatureQuerySet,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
) -> Optional[pd.DataFrame]:
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

    Returns
    -------
    Optional[pd.DataFrame]
    """
    session = session_handler.session
    source_info = session.get_source_info()
    total_num_nodes = feature_query_set.get_num_nodes()
    materialized_feature_table = []
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
    all_node_names = generator.get_node_names()
    node_groups = generator.split_nodes(all_node_names, NUM_FEATURES_PER_QUERY, source_info)
    feature_set_table_name_prefix = f"__TEMP_{ObjectId()}"
    try:
        coroutines = []
        for i, nodes_group in enumerate(node_groups):
            feature_set_table_name = f"{feature_set_table_name_prefix}_{i}"
            feature_query = generator.generate_feature_query(
                node_names=[node.name for node in nodes_group],
                table_name=feature_set_table_name,
            )
            coroutines.append(
                execute_feature_query(
                    session=session,
                    feature_query=feature_query,
                    done_callback=_progress_callback,
                )
            )
            materialized_feature_table.append(feature_query.table_name)
        if coroutines:
            with timer("Execute feature queries", logger=logger):
                feature_query_results = await run_coroutines(
                    coroutines,
                    session_handler.redis,
                    str(session_handler.feature_store.id),
                    session_handler.feature_store.max_query_concurrency,
                )

        for feature_query_result in feature_query_results:
            if isinstance(feature_query_result, FeatureQuery):
                feature_query_set.add_completed_feature_query(feature_query_result)

        failed_node_names = list(
            set(all_node_names) - set(feature_query_set.get_completed_node_names())
        )
        # Note: Later we will set return_exceptions=True in run_coroutines and handle failed nodes
        # here. For now failed_node_names should always be empty, otherwise the error would be
        # raised immediately by run_coroutines.
        assert not failed_node_names, f"Failed to execute feature queries for {failed_node_names}"

        output_query = feature_query_set.construct_output_query(session.get_source_info())
        result = await session.execute_query_long_running(output_query)
        if (
            feature_query_set.output_include_row_index
            and feature_query_set.output_table_name is not None
        ):
            await validate_output_row_index(session, feature_query_set.output_table_name)
        if output_query_progress_callback:
            await output_query_progress_callback(100, feature_query_set.progress_message)
        return result

    finally:
        for table_name in materialized_feature_table:
            await session.drop_table(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=table_name,
                if_exists=True,
            )
