"""
Session related helper functions
"""
from __future__ import annotations

from typing import Any, Callable, Coroutine, Optional, Union

import pandas as pd
from sqlglot.expressions import Expression

from featurebyte.enum import SourceType
from featurebyte.models.feature_query_set import FeatureQuerySet
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.session.base import BaseSession


def _to_query_str(query: Union[str, Expression], source_type: SourceType) -> str:
    if isinstance(query, str):
        return query
    assert isinstance(query, Expression)
    return sql_to_string(query, source_type)


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
            materialized_feature_table.append(feature_query.table_name)
            if progress_callback:
                await progress_callback(
                    int(100 * (i + 1) / total_num_queries),
                    feature_query_set.progress_message,
                )

        result = await session.execute_query_long_running(
            _to_query_str(feature_query_set.output_query, session.source_type)
        )
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
