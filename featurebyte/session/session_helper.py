"""
Session related helper functions
"""
from __future__ import annotations

from typing import Any, Callable, Coroutine, List, Optional, Union

import asyncio

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.common.utils import timer
from featurebyte.enum import InternalName, SourceType
from featurebyte.logging import get_logger
from featurebyte.models.feature_query_set import FeatureQuerySet
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


def _to_query_str(query: Union[str, Expression], source_type: SourceType) -> str:
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
    ValueError
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
        raise ValueError("Row index column is invalid in the output table")


async def run_coroutines(coroutines: List[Coroutine[Any, Any, None]]) -> None:
    """
    Execute the provided list of coroutines

    Parameters
    ----------
    coroutines: List[Coroutine[Any, Any, None]]
        List of coroutines to be executed

    Raises
    ------
    Exception
        If any task failed
    """
    tasks = [asyncio.create_task(coro) for coro in coroutines]
    try:
        await asyncio.gather(*tasks)
    except Exception:
        logger.error(
            "Canceling all other tasks because at least one task failed",
            exc_info=True,
        )
        for task in tasks:
            task.cancel()
        raise


async def execute_feature_query_set(
    session: BaseSession,
    feature_query_set: FeatureQuerySet,
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]] = None,
) -> Optional[pd.DataFrame]:
    """
    Execute the feature queries to materialize features

    Parameters
    ----------
    session: BaseSession
        Session object
    feature_query_set: FeatureQuerySet
        FeatureQuerySet object
    progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
        Optional progress callback function

    Returns
    -------
    Optional[pd.DataFrame]
    """
    total_num_queries = len(feature_query_set.feature_queries) + 1
    materialized_feature_table = []
    try:
        for i, feature_query in enumerate(feature_query_set.feature_queries):
            await session.execute_query_long_running(
                _to_query_str(feature_query.sql, session.source_type)
            )
            await validate_output_row_index(session, feature_query.table_name)
            materialized_feature_table.append(feature_query.table_name)
            if progress_callback:
                await progress_callback(
                    int(100 * (i + 1) / total_num_queries),
                    feature_query_set.progress_message,
                )

        result = await session.execute_query_long_running(
            _to_query_str(feature_query_set.output_query, session.source_type)
        )
        if feature_query_set.validate_output_row_index:
            assert feature_query_set.output_table_name is not None
            await validate_output_row_index(session, feature_query_set.output_table_name)
        if progress_callback:
            await progress_callback(100, feature_query_set.progress_message)
        return result

    finally:
        for table_name in materialized_feature_table:
            await session.drop_table(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=table_name,
                if_exists=True,
            )
