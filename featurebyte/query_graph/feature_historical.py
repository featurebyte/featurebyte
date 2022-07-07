"""
Historical features SQL generation
"""
from __future__ import annotations

import datetime

import pandas as pd
from pandas.api.types import is_datetime64_dtype
from snowflake.connector.pandas_tools import write_pandas

from featurebyte.api.feature import Feature
from featurebyte.enum import SpecialColumnName
from featurebyte.errors import MissingPointInTimeColumnError, TooRecentPointInTimeError
from featurebyte.logger import logger
from featurebyte.query_graph.feature_common import REQUEST_TABLE_NAME
from featurebyte.query_graph.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.session.base import BaseSession

HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR = 48


def get_schema_from_dtypes(df, date_cols):
    schema = []
    for colname, dtype in df.dtypes.to_dict().items():
        if colname in date_cols:
            if dtype != object:
                raise RuntimeError(
                    f"Should convert dtype of {colname} to object first (now is {dtype})"
                )
            db_type = "DATETIME"
        elif dtype == float:
            db_type = "DOUBLE"
        elif dtype == int:
            db_type = "INT"
        elif dtype == object:
            db_type = "VARCHAR"
        else:
            raise RuntimeError(f"Unhandled type: {dtype}")
        schema.append(f"{colname} {db_type}")
    schema = ", ".join(schema)
    return schema


def register_temp_table(session, df, table_name, date_cols) -> None:
    assert isinstance(date_cols, list)
    session.execute_query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        """
    )
    schema = get_schema_from_dtypes(df, date_cols)
    session.execute_query(
        f"""
        CREATE OR REPLACE TEMP TABLE {table_name}(
            {schema}
        )
        """
    )
    write_pandas(session.connection, df, table_name)


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


def validate_historical_requests_point_in_time(training_events: pd.DataFrame) -> None:

    point_in_time_series = training_events[SpecialColumnName.POINT_IN_TIME]
    if not is_datetime64_dtype(point_in_time_series):
        point_in_time_series = pd.to_datetime(point_in_time_series)

    latest_point_in_time = point_in_time_series.max()
    recency = datetime.datetime.now() - latest_point_in_time
    if recency <= pd.Timedelta(HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR, unit="h"):
        raise TooRecentPointInTimeError(
            f"The latest point in time ({latest_point_in_time}) should not be more recent than "
            f"{HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR} hours from now"
        )


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
    validate_historical_requests_point_in_time(training_events)

    # Generate SQL code that computes the features
    sql = get_historical_features_sql(
        feature_objects=feature_objects, request_table_columns=training_events.columns.tolist()
    )
    logger.debug(f"Historical features SQL:\n{sql}")

    # Execute feature SQL code
    session = get_session_from_feature_objects(feature_objects)
    register_temp_table(
        session=session,
        df=training_events,
        table_name=REQUEST_TABLE_NAME,
        date_cols=[SpecialColumnName.POINT_IN_TIME],
    )
    return session.execute_query(sql)
