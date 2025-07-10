"""
Historical features SQL generation
"""

from __future__ import annotations

import datetime
from abc import ABC, abstractmethod
from typing import List, Optional, cast

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
from sqlglot import expressions

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.exception import MissingPointInTimeColumnError, TooRecentPointInTimeError
from featurebyte.logging import get_logger
from featurebyte.models.column_statistics import ColumnStatisticsInfo
from featurebyte.models.feature_query_set import (
    FeatureQueryGenerator,
    FeatureQuerySet,
)
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.models.tile import OnDemandTileTable
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import (
    PartitionColumnFilters,
    get_fully_qualified_table_name,
)
from featurebyte.query_graph.sql.cron import JobScheduleTableSet
from featurebyte.query_graph.sql.feature_compute import (
    FeatureExecutionPlanner,
    FeatureQuery,
    FeatureQueryPlan,
)
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.session.base import BaseSession

HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR = 48

PROGRESS_MESSAGE_COMPUTING_FEATURES = "Computing features"
PROGRESS_MESSAGE_COMPUTING_TARGET = "Computing target"
TILE_COMPUTE_PROGRESS_MAX_PERCENT = 50  #  Progress percentage to report at end of tile computation


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
        self,
        session: BaseSession,
        request_table_name: str,
        add_row_index: bool,
    ) -> None:
        """
        Register the observation set as the request table in the session

        Parameters
        ----------
        session : BaseSession
            Session
        request_table_name : str
            Request table name
        add_row_index : bool
            Whether to add row index column TABLE_ROW_INDEX to the request table. This is
            needed when the historical features are materialized in batches.
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
        self, session: BaseSession, request_table_name: str, add_row_index: bool
    ) -> None:
        if add_row_index:
            self.dataframe[InternalName.TABLE_ROW_INDEX] = np.arange(self.dataframe.shape[0])
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
        self, session: BaseSession, request_table_name: str, add_row_index: bool
    ) -> None:
        columns = ["*"]

        if add_row_index and not self.observation_table.has_row_index:
            row_number = expressions.Window(
                this=expressions.Anonymous(this="ROW_NUMBER"),
                order=expressions.Order(expressions=[expressions.Literal.number(1)]),
            )
            columns.append(
                expressions.alias_(row_number, alias=InternalName.TABLE_ROW_INDEX, quoted=True),
            )

        await session.create_table_as(
            table_details=TableDetails(table_name=request_table_name),
            select_expr=expressions.select(*columns).from_(
                get_fully_qualified_table_name(
                    self.observation_table.location.table_details.model_dump()
                )
            ),
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
    recency = datetime.datetime.utcnow() - latest_point_in_time
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
    source_info: SourceInfo,
    serving_names_mapping: dict[str, str] | None = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    on_demand_tile_tables: Optional[list[OnDemandTileTable]] = None,
    job_schedule_table_set: Optional[JobScheduleTableSet] = None,
    column_statistics_info: Optional[ColumnStatisticsInfo] = None,
    partition_column_filters: Optional[PartitionColumnFilters] = None,
) -> FeatureQueryPlan:
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
    source_info: SourceInfo
        Source information
    serving_names_mapping : dict[str, str] | None
        Optional mapping from original serving name to new serving name
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    on_demand_tile_tables: Optional[list[OnDemandTileTable]]
        List of on-demand tile tables if available
    job_schedule_table_set: Optional[JobScheduleTableSet]
        Job schedule table set if available. These will be used to compute features that are using
        a cron-based feature job setting.
    column_statistics_info: Optional[ColumnStatisticsInfo]
        Column statistics information
    partition_column_filters: Optional[PartitionColumnFilters]
        Partition column filters to apply to the source tables

    Returns
    -------
    FeatureQueryPlan
    """
    planner = FeatureExecutionPlanner(
        graph,
        serving_names_mapping=serving_names_mapping,
        source_info=source_info,
        is_online_serving=False,
        parent_serving_preparation=parent_serving_preparation,
        on_demand_tile_tables=on_demand_tile_tables,
        job_schedule_table_set=job_schedule_table_set,
        column_statistics_info=column_statistics_info,
        partition_column_filters=partition_column_filters,
    )
    plan = planner.generate_plan(nodes)

    historical_features_sql = plan.construct_combined_sql(
        request_table_name=request_table_name,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=request_table_columns,
    )
    return historical_features_sql


class HistoricalFeatureQueryGenerator(FeatureQueryGenerator):
    """
    Historical feature query generator
    """

    def __init__(
        self,
        graph: QueryGraph,
        nodes: list[Node],
        request_table_name: str,
        request_table_columns: list[str],
        source_info: SourceInfo,
        output_table_details: TableDetails,
        output_feature_names: list[str],
        output_include_row_index: bool = False,
        serving_names_mapping: dict[str, str] | None = None,
        parent_serving_preparation: Optional[ParentServingPreparation] = None,
        on_demand_tile_tables: Optional[list[OnDemandTileTable]] = None,
        job_schedule_table_set: Optional[JobScheduleTableSet] = None,
        column_statistics_info: Optional[ColumnStatisticsInfo] = None,
        partition_column_filters: Optional[PartitionColumnFilters] = None,
    ):
        self.request_table_name = request_table_name
        self.graph = graph
        self.nodes = nodes
        self.request_table_columns = request_table_columns
        self.source_info = source_info
        self.output_table_details = output_table_details
        self.output_feature_names = output_feature_names
        self.serving_names_mapping = serving_names_mapping
        self.parent_serving_preparation = parent_serving_preparation
        self.on_demand_tile_tables = on_demand_tile_tables
        self.job_schedule_table_set = job_schedule_table_set
        self.output_include_row_index = output_include_row_index
        self.column_statistics_info = column_statistics_info
        self.partition_column_filters = partition_column_filters

    def get_query_graph(self) -> QueryGraph:
        return self.graph

    def get_nodes(self) -> list[Node]:
        return self.nodes

    def generate_feature_query(self, node_names: list[str], table_name: str) -> FeatureQuery:
        nodes = [self.graph.get_node_by_name(node_name) for node_name in node_names]
        feature_set_sql = get_historical_features_expr(
            graph=self.graph,
            nodes=nodes,
            request_table_columns=[InternalName.TABLE_ROW_INDEX.value] + self.request_table_columns,
            serving_names_mapping=self.serving_names_mapping,
            source_info=self.source_info,
            request_table_name=self.request_table_name,
            parent_serving_preparation=self.parent_serving_preparation,
            on_demand_tile_tables=self.on_demand_tile_tables,
            job_schedule_table_set=self.job_schedule_table_set,
            column_statistics_info=self.column_statistics_info,
            partition_column_filters=self.partition_column_filters,
        )
        feature_query = feature_set_sql.get_feature_query(
            table_name=table_name,
            node_names=node_names,
            source_info=self.source_info,
        )
        return feature_query


def get_historical_features_query_set(
    request_table_name: str,
    graph: QueryGraph,
    nodes: list[Node],
    request_table_columns: list[str],
    source_info: SourceInfo,
    output_table_details: TableDetails,
    output_feature_names: list[str],
    serving_names_mapping: dict[str, str] | None = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    on_demand_tile_tables: Optional[list[OnDemandTileTable]] = None,
    job_schedule_table_set: Optional[JobScheduleTableSet] = None,
    column_statistics_info: Optional[ColumnStatisticsInfo] = None,
    partition_column_filters: Optional[PartitionColumnFilters] = None,
    output_include_row_index: bool = False,
    progress_message: str = PROGRESS_MESSAGE_COMPUTING_FEATURES,
) -> FeatureQuerySet:
    """Construct the SQL code that extracts historical features

    Parameters
    ----------
    request_table_name : str
        Name of request table to use
    graph : QueryGraph
        Query graph
    nodes : list[Node]
        List of query graph nodes
    request_table_columns : list[str]
        List of column names in the training events
    source_info: SourceInfo
        Source information
    output_table_details: TableDetails
        Output table details to write the results to
    output_feature_names : list[str]
        List of output feature names
    serving_names_mapping : dict[str, str] | None
        Optional mapping from original serving name to new serving name
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    on_demand_tile_tables: Optional[list[OnDemandTileTable]]
        List of on-demand tile tables if available
    job_schedule_table_set: Optional[JobScheduleTableSet]
        Job schedule table set if available. These will be used to compute features that are using
        a cron-based feature job setting.
    column_statistics_info: Optional[ColumnStatisticsInfo]
        Column statistics information
    partition_column_filters: Optional[PartitionColumnFilters]
        Partition column filters to apply to the source tables
    output_include_row_index: bool
        Whether to include the TABLE_ROW_INDEX column in the output
    progress_message : str
        Customised progress message which will be sent to a client.

    Returns
    -------
    FeatureQuerySet
    """
    feature_query_generator = HistoricalFeatureQueryGenerator(
        graph=graph,
        nodes=nodes,
        request_table_name=request_table_name,
        request_table_columns=request_table_columns,
        source_info=source_info,
        output_table_details=output_table_details,
        output_feature_names=output_feature_names,
        output_include_row_index=output_include_row_index,
        serving_names_mapping=serving_names_mapping,
        parent_serving_preparation=parent_serving_preparation,
        on_demand_tile_tables=on_demand_tile_tables,
        job_schedule_table_set=job_schedule_table_set,
        column_statistics_info=column_statistics_info,
        partition_column_filters=partition_column_filters,
    )
    feature_query_set = FeatureQuerySet(
        feature_query_generator=feature_query_generator,
        request_table_name=request_table_name,
        request_table_columns=request_table_columns,
        output_table_details=output_table_details,
        output_feature_names=output_feature_names,
        output_include_row_index=output_include_row_index,
        progress_message=progress_message,
    )
    return feature_query_set
