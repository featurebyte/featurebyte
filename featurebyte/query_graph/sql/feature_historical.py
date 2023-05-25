"""
Historical features SQL generation
"""
from __future__ import annotations

from typing import Callable, List, Optional, Union, cast

import datetime
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

import numpy as np
import pandas as pd
from bson import ObjectId
from pandas.api.types import is_datetime64_any_dtype
from sqlglot import expressions

from featurebyte.enum import SourceType, SpecialColumnName
from featurebyte.exception import MissingPointInTimeColumnError, TooRecentPointInTimeError
from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import (
    REQUEST_TABLE_NAME,
    get_fully_qualified_table_name,
    get_qualified_column_identifier,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.sql.parent_serving import construct_request_table_with_parent_entities
from featurebyte.session.base import BaseSession
from featurebyte.tile.manager import TILE_COMPUTE_PROGRESS_MAX_PERCENT
from featurebyte.tile.tile_cache import TileCache

HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR = 48
NUM_FEATURES_PER_QUERY = 50
FB_ROW_INDEX_FOR_JOIN = "__FB_ROW_INDEX_FOR_JOIN"


logger = get_logger(__name__)


class ObservationSet(ABC):
    """
    Observation set abstraction for historical features (used internally in this module)
    """

    @property
    @abstractmethod
    def columns(self) -> list[str]:
        """
        List of columns available in the observation set

        Returns
        -------
        list[str]
        """

    @property
    @abstractmethod
    def most_recent_point_in_time(self) -> pd.Timestamp:
        """
        The most recent point in time in the observation set
        """

    @abstractmethod
    async def register_as_request_table(
        self, session: BaseSession, request_table_name: str
    ) -> None:
        """
        Register the observation set as the request table in the session

        Parameters
        ----------
        session : BaseSession
            Session
        request_table_name : str
            Request table name
        """


class DataFrameObservationSet(ObservationSet):
    """
    Observation set based on an in memory pandas DataFrame
    """

    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = convert_point_in_time_dtype_if_needed(dataframe)

    @property
    def columns(self) -> List[str]:
        return cast(List[str], self.dataframe.columns.tolist())

    @property
    def most_recent_point_in_time(self) -> pd.Timestamp:
        return self.dataframe[SpecialColumnName.POINT_IN_TIME].max()

    async def register_as_request_table(
        self, session: BaseSession, request_table_name: str
    ) -> None:
        self.dataframe[FB_ROW_INDEX_FOR_JOIN] = np.arange(self.dataframe.shape[0])
        await session.register_table(request_table_name, self.dataframe)


class MaterializedTableObservationSet(ObservationSet):
    """
    Observation set based on a materialized table in data warehouse
    """

    def __init__(self, observation_table: ObservationTableModel):
        self.observation_table = observation_table

    @property
    def columns(self) -> list[str]:
        return [col.name for col in self.observation_table.columns_info]

    @property
    def most_recent_point_in_time(self) -> pd.Timestamp:
        return pd.to_datetime(self.observation_table.most_recent_point_in_time)

    async def register_as_request_table(
        self, session: BaseSession, request_table_name: str
    ) -> None:
        row_number = expressions.Window(
            this=expressions.Anonymous(this="ROW_NUMBER"),
            order=expressions.Order(expressions=[expressions.Literal.number(1)]),
        )
        query = sql_to_string(
            expressions.select(
                "*",
                expressions.alias_(row_number, alias=FB_ROW_INDEX_FOR_JOIN, quoted=True),
            ).from_(
                get_fully_qualified_table_name(self.observation_table.location.table_details.dict())
            ),
            source_type=session.source_type,
        )
        await session.register_table_with_query(request_table_name, query)


@dataclass
class FeatureSet:
    expr: expressions.Select
    table_name: str
    feature_names: list[str]


@dataclass
class HistoricalFeatureQuerySet:
    feature_sets: list[FeatureSet]
    output_expr: expressions.Select

    async def execute(self, session: BaseSession, output_table_details: TableDetails) -> None:
        materialized_feature_table = []
        try:
            for feature_set in self.feature_sets:
                expression = get_sql_adapter(session.source_type).create_table_as(
                    table_details=TableDetails(table_name=feature_set.table_name),
                    select_expr=feature_set.expr,
                )
                query = sql_to_string(expression, source_type=session.source_type)
                await session.execute_query_long_running(query)
                materialized_feature_table.append(feature_set.table_name)

            expression = get_sql_adapter(session.source_type).create_table_as(
                table_details=output_table_details,
                select_expr=self.output_expr,
            )
            query = sql_to_string(expression, source_type=session.source_type)
            await session.execute_query_long_running(query)

        finally:
            for table_name in materialized_feature_table:
                await session.drop_table(
                    database_name=session.database_name,
                    schema_name=session.schema_name,
                    table_name=table_name,
                    if_exists=True,
                )


def get_internal_observation_set(
    observation_set: pd.DataFrame | ObservationTableModel,
) -> ObservationSet:
    """
    Get the internal observation set representation

    Parameters
    ----------
    observation_set : pd.DataFrame | ObservationTableModel
        Observation set

    Returns
    -------
    ObservationSet
    """
    if isinstance(observation_set, pd.DataFrame):
        return DataFrameObservationSet(observation_set)
    return MaterializedTableObservationSet(observation_set)


def convert_point_in_time_dtype_if_needed(observation_set: pd.DataFrame) -> pd.DataFrame:
    """
    Check dtype of the point in time column and convert if necessary. The converted DataFrame will
    later be used to create a temp table in the session.

    Parameters
    ----------
    observation_set : pd.DataFrame
        Observation set

    Returns
    -------
    pd.DataFrame
    """
    observation_set = observation_set.copy()

    if SpecialColumnName.POINT_IN_TIME not in observation_set.columns:
        return observation_set

    if not is_datetime64_any_dtype(observation_set[SpecialColumnName.POINT_IN_TIME]):
        observation_set[SpecialColumnName.POINT_IN_TIME] = pd.to_datetime(
            observation_set[SpecialColumnName.POINT_IN_TIME]
        )

    # convert point in time to tz-naive UTC timestamps
    observation_set = observation_set
    observation_set[SpecialColumnName.POINT_IN_TIME] = pd.to_datetime(
        observation_set[SpecialColumnName.POINT_IN_TIME], utc=True
    ).dt.tz_localize(None)

    return observation_set


def validate_historical_requests_point_in_time(observation_set: ObservationSet) -> None:
    """Validate the point in time column in the request input and perform type conversion if needed

    A copy will be made if the point in time column does not already have timestamp dtype.

    Parameters
    ----------
    observation_set: ObservationSet
        Observation set

    Raises
    ------
    TooRecentPointInTimeError
        If any of the provided point in time values are too recent
    """
    # Latest point in time must be older than 48 hours
    latest_point_in_time = observation_set.most_recent_point_in_time
    recency = datetime.datetime.now() - latest_point_in_time
    if recency <= pd.Timedelta(HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR, unit="h"):
        raise TooRecentPointInTimeError(
            f"The latest point in time ({latest_point_in_time}) should not be more recent than "
            f"{HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR} hours from now"
        )


def validate_request_schema(observation_set: ObservationSet) -> None:
    """Validate observation set schema

    Parameters
    ----------
    observation_set: ObservationSet
        Observation set

    Raises
    ------
    MissingPointInTimeColumnError
        If point in time column is not provided
    """
    if SpecialColumnName.POINT_IN_TIME not in observation_set.columns:
        raise MissingPointInTimeColumnError("POINT_IN_TIME column is required")


def get_historical_features_expr(
    request_table_name: str,
    graph: QueryGraph,
    nodes: list[Node],
    request_table_columns: list[str],
    source_type: SourceType,
    serving_names_mapping: dict[str, str] | None = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
) -> expressions.Select:
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
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features

    Returns
    -------
    expressions.Select
    """
    planner = FeatureExecutionPlanner(
        graph,
        serving_names_mapping=serving_names_mapping,
        source_type=source_type,
        is_online_serving=False,
        parent_serving_preparation=parent_serving_preparation,
    )
    plan = planner.generate_plan(nodes)

    return plan.construct_combined_sql(
        request_table_name=request_table_name,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=request_table_columns,
    )


def split_nodes(nodes: list[Node]) -> list[list[Node]]:
    result = []
    for i in range(0, len(nodes), NUM_FEATURES_PER_QUERY):
        current_nodes = nodes[i : i + NUM_FEATURES_PER_QUERY]
        result.append(current_nodes)
    return result


def construct_join_feature_sets_query(
    feature_sets: list[FeatureSet],
    request_table_name: str,
    request_table_columns: list[str],
) -> expressions.Select:
    expr = expressions.select(
        *(get_qualified_column_identifier(col, "REQ") for col in request_table_columns)
    ).from_(f"{request_table_name} AS REQ")
    for i, feature_set in enumerate(feature_sets):
        table_alias = f"T{i}"
        expr = expr.join(
            expressions.Table(
                this=quoted_identifier(feature_set.table_name),
                alias=expressions.TableAlias(this=expressions.Identifier(this=table_alias)),
            ),
            join_type="left",
            on=expressions.EQ(
                this=get_qualified_column_identifier(FB_ROW_INDEX_FOR_JOIN, "REQ"),
                expression=get_qualified_column_identifier(FB_ROW_INDEX_FOR_JOIN, table_alias),
            ),
        )
        expr = expr.select(*[f'{table_alias}."{name}"' for name in feature_set.feature_names])
    return expr


def get_historical_features_query_set(
    request_table_name: str,
    graph: QueryGraph,
    nodes: list[Node],
    request_table_columns: list[str],
    source_type: SourceType,
    serving_names_mapping: dict[str, str] | None = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
) -> HistoricalFeatureQuerySet:
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
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features

    Returns
    -------
    HistoricalFeatureQuerySet
    """
    feature_sets = []
    node_groups = split_nodes(nodes)
    for nodes_group in node_groups:
        planner = FeatureExecutionPlanner(
            graph,
            serving_names_mapping=serving_names_mapping,
            source_type=source_type,
            is_online_serving=False,
            parent_serving_preparation=parent_serving_preparation,
        )
        plan = planner.generate_plan(nodes_group)
        feature_set_expr = plan.construct_combined_sql(
            request_table_name=request_table_name,
            point_in_time_column=SpecialColumnName.POINT_IN_TIME,
            request_table_columns=[FB_ROW_INDEX_FOR_JOIN] + request_table_columns,
        )
        feature_set_table_name = f"__TEMP_{ObjectId()}"
        feature_sets.append(
            FeatureSet(
                expr=feature_set_expr,
                table_name=feature_set_table_name,
                feature_names=plan.feature_names,
            )
        )
    output_expr = construct_join_feature_sets_query(
        feature_sets=feature_sets,
        request_table_name=request_table_name,
        request_table_columns=request_table_columns,
    )
    return HistoricalFeatureQuerySet(
        feature_sets=feature_sets,
        output_expr=output_expr,
    )


async def compute_tiles_on_demand(
    session: BaseSession,
    graph: QueryGraph,
    nodes: list[Node],
    request_id: str,
    request_table_name: str,
    request_table_columns: list[str],
    serving_names_mapping: Optional[dict[str, str]],
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str], None]] = None,
) -> None:
    """
    Compute tiles on demand

    Parameters
    ----------
    session: BaseSession
        Session to use to make queries
    graph: QueryGraph
        Query graph
    nodes: list[Node]
        List of query graph node
    request_id: str
        Request ID to be used as suffix of table names when creating temporary tables
    request_table_name: str
        Name of request table
    request_table_columns: list[str]
        List of column names in the observations set
    serving_names_mapping : dict[str, str] | None
        Optional serving names mapping if the training events data has different serving name
        columns than those defined in Entities
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    progress_callback: Optional[Callable[[int, str], None]]
        Optional progress callback function
    """
    tile_cache = TileCache(session=session)

    if parent_serving_preparation is None:
        effective_request_table_name = request_table_name
    else:
        # Lookup parent entities and join them with the request table since tile computation
        # requires these entity columns to be present in the request table.
        request_table_expr, _ = construct_request_table_with_parent_entities(
            request_table_name=request_table_name,
            request_table_columns=request_table_columns,
            join_steps=parent_serving_preparation.join_steps,
            feature_store_details=parent_serving_preparation.feature_store_details,
        )
        request_table_query = sql_to_string(request_table_expr, session.source_type)
        effective_request_table_name = "JOINED_PARENTS_" + request_table_name
        await session.register_table_with_query(
            effective_request_table_name,
            request_table_query,
        )

    await tile_cache.compute_tiles_on_demand(
        graph=graph,
        nodes=nodes,
        request_id=request_id,
        request_table_name=effective_request_table_name,
        serving_names_mapping=serving_names_mapping,
        progress_callback=progress_callback,
    )


async def get_historical_features(
    session: BaseSession,
    graph: QueryGraph,
    nodes: list[Node],
    observation_set: Union[pd.DataFrame, ObservationTableModel],
    source_type: SourceType,
    output_table_details: TableDetails,
    serving_names_mapping: dict[str, str] | None = None,
    is_feature_list_deployed: bool = False,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    progress_callback: Optional[Callable[[int, str], None]] = None,
) -> None:
    """Get historical features

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
    source_type : SourceType
        Source type information
    serving_names_mapping : dict[str, str] | None
        Optional serving names mapping if the observations set has different serving name columns
        than those defined in Entities
    is_feature_list_deployed : bool
        Whether the feature list that triggered this historical request is deployed. If so, tile
        tables would have already been back-filled and there is no need to check and calculate tiles
        on demand.
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    output_table_details: TableDetails
        Output table details to write the results to
    progress_callback: Optional[Callable[[int, str], None]]
        Optional progress callback function
    """
    tic_ = time.time()

    observation_set = get_internal_observation_set(observation_set)

    # Validate request
    validate_request_schema(observation_set)
    validate_historical_requests_point_in_time(observation_set)

    # use a unique request table name
    request_id = session.generate_session_unique_id()
    request_table_name = f"{REQUEST_TABLE_NAME}_{request_id}"

    # Generate SQL code that computes the features
    request_table_columns = observation_set.columns
    sql_expr = get_historical_features_expr(
        graph=graph,
        nodes=nodes,
        request_table_columns=request_table_columns,
        serving_names_mapping=serving_names_mapping,
        source_type=source_type,
        request_table_name=request_table_name,
        parent_serving_preparation=parent_serving_preparation,
    )

    # Execute feature SQL code
    await observation_set.register_as_request_table(session, request_table_name)

    # Compute tiles on demand if required
    if not is_feature_list_deployed:
        tic = time.time()
        for nodes_group in split_nodes(nodes):
            logger.debug(f"Checking and computing tiles on demand for {len(nodes_group)} nodes")
            await compute_tiles_on_demand(
                session=session,
                graph=graph,
                nodes=nodes_group,
                request_id=request_id,
                request_table_name=request_table_name,
                request_table_columns=request_table_columns,
                serving_names_mapping=serving_names_mapping,
                parent_serving_preparation=parent_serving_preparation,
                progress_callback=progress_callback,
            )
        elapsed = time.time() - tic
        logger.debug(f"Checking and computing tiles on demand took {elapsed:.2f}s")

    if progress_callback:
        progress_callback(TILE_COMPUTE_PROGRESS_MAX_PERCENT, "Computing features")

    historical_feature_query_set = get_historical_features_query_set(
        graph=graph,
        nodes=nodes,
        request_table_columns=request_table_columns,
        serving_names_mapping=serving_names_mapping,
        source_type=source_type,
        request_table_name=request_table_name,
        parent_serving_preparation=parent_serving_preparation,
    )
    await historical_feature_query_set.execute(session, output_table_details)
    logger.debug(f"compute_historical_features in total took {time.time() - tic_:.2f}s")
