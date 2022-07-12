"""
Historical features SQL generation
"""
from __future__ import annotations

from typing import Optional

import datetime
import time

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from featurebyte.api.feature import Feature
from featurebyte.api.feature_store import FeatureStore
from featurebyte.config import Credentials
from featurebyte.enum import SpecialColumnName
from featurebyte.exception import MissingPointInTimeColumnError, TooRecentPointInTimeError
from featurebyte.logger import logger
from featurebyte.query_graph.feature_common import REQUEST_TABLE_NAME
from featurebyte.query_graph.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.session.base import BaseSession
from featurebyte.tile.tile_cache import SnowflakeTileCache

HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR = 48


def get_session_from_feature_objects(
    feature_objects: list[Feature], credentials: Credentials | None = None
) -> BaseSession:
    """Get a session object from a list of Feature objects

    Parameters
    ----------
    feature_objects : list[Feature]
        Feature objects
    credentials : Credentials | None
        Optional feature store to credential mapping

    Returns
    -------
    BaseSession

    Raises
    ------
    NotImplementedError
        If the list of Features do not all use the same store
    """
    feature_store: Optional[FeatureStore] = None
    for feature in feature_objects:
        store = feature.tabular_source[0]
        assert isinstance(store, FeatureStore)
        if feature_store is None:
            feature_store = store
        elif feature_store != store:
            raise NotImplementedError(
                "Historical features request using multiple stores not supported"
            )
    return feature_objects[0].get_session(credentials=credentials)


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
    feature_objects: list[Feature],
    request_table_columns: list[str],
) -> str:
    """Construct the SQL code that extracts historical features

    Parameters
    ----------
    feature_objects : list[Feature]
        List of Feature objects
    request_table_columns : list[str]
        List of column names in the training events

    Returns
    -------
    str
    """
    feature_nodes = [feature.node for feature in feature_objects]
    planner = FeatureExecutionPlanner(GlobalQueryGraph())
    plan = planner.generate_plan(feature_nodes)
    sql = plan.construct_combined_sql(
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=request_table_columns,
    )
    return sql


def get_historical_features(
    feature_objects: list[Feature],
    training_events: pd.DataFrame,
    credentials: Credentials | None = None,
) -> pd.DataFrame:
    """Get historical features

    Parameters
    ----------
    feature_objects : list[Feature]
        List of Feature objects
    training_events : pd.DataFrame
        Training events DataFrame
    credentials : Credentials | None
        Optional feature store to credential mapping

    Returns
    -------
    pd.DataFrame
    """
    tic_ = time.time()

    # Validate request
    validate_request_schema(training_events)
    training_events = validate_historical_requests_point_in_time(training_events)

    # Generate SQL code that computes the features
    sql = get_historical_features_sql(
        feature_objects=feature_objects, request_table_columns=training_events.columns.tolist()
    )
    logger.debug(f"Historical features SQL:\n{sql}")

    # Execute feature SQL code
    session = get_session_from_feature_objects(feature_objects, credentials=credentials)
    session.register_temp_table(REQUEST_TABLE_NAME, training_events)

    # Compute tiles on demand if required
    tic = time.time()
    tile_cache = SnowflakeTileCache(session=session)
    tile_cache.compute_tiles_on_demand(features=feature_objects)
    elapsed = time.time() - tic
    logger.debug(f"Checking and computing tiles on demand took {elapsed:.2f}s")

    logger.debug(f"get_historical_features took {time.time() - tic_:.2f}s")
    return session.execute_query(sql)
