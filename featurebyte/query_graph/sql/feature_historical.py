"""
Historical features SQL generation
"""
from __future__ import annotations

from typing import AsyncGenerator

import datetime
import time

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from featurebyte.enum import SourceType, SpecialColumnName
from featurebyte.exception import (
    MissingPointInTimeColumnError,
    MissingServingNameError,
    TooRecentPointInTimeError,
)
from featurebyte.logger import logger
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.session.base import BaseSession
from featurebyte.tile.tile_cache import get_tile_cache

HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR = 48


def validate_historical_requests_point_in_time(training_events: pd.DataFrame) -> pd.DataFrame:
    """Validate the point in time column in the request input and perform type conversion if needed

    A copy will be made if the point in time column does not already have timestamp dtype.

    Parameters
    ----------
    training_events : pd.DataFrame
        Training events

    Returns
    -------
    pd.DataFrame

    Raises
    ------
    TooRecentPointInTimeError
        If any of the provided point in time values are too recent
    """

    # Check dtype and convert if necessary. The converted DataFrame will later be used to create a
    # temp table in the session.
    if not is_datetime64_any_dtype(training_events[SpecialColumnName.POINT_IN_TIME]):
        training_events = training_events.copy()
        training_events[SpecialColumnName.POINT_IN_TIME] = pd.to_datetime(
            training_events[SpecialColumnName.POINT_IN_TIME]
        )

    # convert point in time to tz-naive UTC timestamps
    training_events = training_events.copy()
    training_events[SpecialColumnName.POINT_IN_TIME] = pd.to_datetime(
        training_events[SpecialColumnName.POINT_IN_TIME], utc=True
    ).dt.tz_localize(None)

    # Latest point in time must be older than 48 hours
    latest_point_in_time = training_events[SpecialColumnName.POINT_IN_TIME].max()
    recency = datetime.datetime.now() - latest_point_in_time
    if recency <= pd.Timedelta(HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR, unit="h"):
        raise TooRecentPointInTimeError(
            f"The latest point in time ({latest_point_in_time}) should not be more recent than "
            f"{HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR} hours from now"
        )

    return training_events


def validate_request_schema(training_events: pd.DataFrame) -> None:
    """Validate training events schema

    Parameters
    ----------
    training_events : DataFrame
        Training events DataFrame

    Raises
    ------
    MissingPointInTimeColumnError
        If point in time column is not provided
    """
    # Currently this only checks the existence of point in time column. Later this should include
    # other validation such as existence of required serving names based on Features' entities.
    if SpecialColumnName.POINT_IN_TIME not in training_events:
        raise MissingPointInTimeColumnError("POINT_IN_TIME column is required")


def get_historical_features_sql(
    request_table_name: str,
    graph: QueryGraph,
    nodes: list[Node],
    request_table_columns: list[str],
    source_type: SourceType,
    serving_names_mapping: dict[str, str] | None = None,
) -> str:
    """Construct the SQL code that extracts historical features

    Parameters
    ----------
    request_table_name : str
        Name of request table to use
    graph : QueryGraph
        Query graph
    nodes : list[Node]
        List of query graph node
    request_table_columns : list[str]
        List of column names in the training events
    source_type : SourceType
        Source type information
    serving_names_mapping : dict[str, str] | None
        Optional mapping from original serving name to new serving name

    Returns
    -------
    str

    Raises
    ------
    MissingServingNameError
        If any required serving name is not provided
    """
    planner = FeatureExecutionPlanner(
        graph,
        serving_names_mapping=serving_names_mapping,
        source_type=source_type,
        is_online_serving=False,
    )
    plan = planner.generate_plan(nodes)

    missing_serving_names = plan.required_serving_names.difference(request_table_columns)
    if missing_serving_names:
        missing_serving_names_str = ", ".join(sorted(missing_serving_names))
        raise MissingServingNameError(
            f"Required serving names not provided: {missing_serving_names_str}"
        )

    sql = plan.construct_combined_sql(
        request_table_name=request_table_name,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=request_table_columns,
    ).sql(pretty=True)

    return sql


async def get_historical_features(
    session: BaseSession,
    graph: QueryGraph,
    nodes: list[Node],
    training_events: pd.DataFrame,
    source_type: SourceType,
    serving_names_mapping: dict[str, str] | None = None,
) -> AsyncGenerator[bytes, None]:
    """Get historical features

    Parameters
    ----------
    session: BaseSession
        Session to use to make queries
    graph : QueryGraph
        Query graph
    nodes : list[Node]
        List of query graph node
    training_events : pd.DataFrame
        Training events DataFramt
    source_type : SourceType
        Source type information
    serving_names_mapping : dict[str, str] | None
        Optional serving names mapping if the training events data has different serving name
        columns than those defined in Entities

    Returns
    -------
    AsyncGenerator[bytes, None]
    """
    tic_ = time.time()

    # Validate request
    validate_request_schema(training_events)
    training_events = validate_historical_requests_point_in_time(training_events)

    # use a unique request table name
    request_id = session.generate_session_unique_id()
    request_table_name = f"{REQUEST_TABLE_NAME}_{request_id}"

    # Generate SQL code that computes the features
    sql = get_historical_features_sql(
        graph=graph,
        nodes=nodes,
        request_table_columns=training_events.columns.tolist(),
        serving_names_mapping=serving_names_mapping,
        source_type=source_type,
        request_table_name=request_table_name,
    )

    # Execute feature SQL code
    await session.register_table(request_table_name, training_events)

    # Compute tiles on demand if required
    tic = time.time()
    tile_cache = get_tile_cache(session=session)
    await tile_cache.compute_tiles_on_demand(
        graph=graph,
        nodes=nodes,
        request_id=request_id,
        serving_names_mapping=serving_names_mapping,
    )
    elapsed = time.time() - tic
    logger.debug(f"Checking and computing tiles on demand took {elapsed:.2f}s")

    # Execute feature query
    tic = time.time()
    result = session.get_async_query_stream(sql)

    elapsed = time.time() - tic
    logger.debug(f"Executing feature query took {elapsed:.2f}s")
    logger.debug(f"get_historical_features in total took {time.time() - tic_:.2f}s")
    return result
