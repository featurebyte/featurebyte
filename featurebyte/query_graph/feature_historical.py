"""
Historical features SQL generation
"""
from __future__ import annotations

import datetime

import pandas as pd
from pandas.api.types import is_datetime64_dtype

from featurebyte.api.feature import Feature
from featurebyte.enum import SpecialColumnName
from featurebyte.errors import MissingPointInTimeColumnError, TooRecentPointInTimeError
from featurebyte.logger import logger
from featurebyte.query_graph.feature_common import REQUEST_TABLE_NAME
from featurebyte.query_graph.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.session.base import BaseSession

HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR = 48


def get_session_from_feature_objects(feature_objects: list[Feature]) -> BaseSession:
    feature_store = None
    for feature in feature_objects:
        store = feature.tabular_source[0]
        if feature_store is None:
            feature_store = store
        elif feature_store != store:
            raise NotImplementedError(
                "Historical features request using multiple stores not supported"
            )
    assert feature_store is not None
    return feature_store.get_session()


def validate_historical_requests_point_in_time(training_events: pd.DataFrame) -> pd.DataFrame:

    if not is_datetime64_dtype(training_events[SpecialColumnName.POINT_IN_TIME]):
        training_events = training_events.copy()
        training_events[SpecialColumnName.POINT_IN_TIME] = pd.to_datetime(
            training_events[SpecialColumnName.POINT_IN_TIME]
        )

    latest_point_in_time = training_events[SpecialColumnName.POINT_IN_TIME].max()
    recency = datetime.datetime.now() - latest_point_in_time
    if recency <= pd.Timedelta(HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR, unit="h"):
        raise TooRecentPointInTimeError(
            f"The latest point in time ({latest_point_in_time}) should not be more recent than "
            f"{HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR} hours from now"
        )

    return training_events


def validate_request_schema(training_events: pd.DataFrame) -> None:
    if SpecialColumnName.POINT_IN_TIME not in training_events:
        raise MissingPointInTimeColumnError("POINT_IN_TIME column is required")


def get_historical_features_sql(
    feature_objects: list[Feature],
    request_table_columns: list[str],
) -> str:
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
) -> pd.DataFrame:

    # Validate request
    validate_request_schema(training_events)
    training_events = validate_historical_requests_point_in_time(training_events)

    # Generate SQL code that computes the features
    sql = get_historical_features_sql(
        feature_objects=feature_objects, request_table_columns=training_events.columns.tolist()
    )
    logger.debug(f"Historical features SQL:\n{sql}")

    # Execute feature SQL code
    session = get_session_from_feature_objects(feature_objects)
    session.register_temp_table(REQUEST_TABLE_NAME, training_events)

    return session.execute_query(sql)
