"""
Historical features SQL generation
"""
from __future__ import annotations

from typing import Callable, List, Optional, Tuple, cast

import datetime
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
    get_fully_qualified_table_name,
    get_qualified_column_identifier,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.sql.specs import NonTileBasedAggregationSpec, TileBasedAggregationSpec
from featurebyte.session.base import BaseSession

HISTORICAL_REQUESTS_POINT_IN_TIME_RECENCY_HOUR = 48
NUM_FEATURES_PER_QUERY = 50
FB_ROW_INDEX_FOR_JOIN = "__FB_ROW_INDEX_FOR_JOIN"

PROGRESS_MESSAGE_COMPUTING_FEATURES = "Computing features"
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
            Whether to add row index column FB_ROW_INDEX_FOR_JOIN to the request table. This is
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
        self, session: BaseSession, request_table_name: str, add_row_index: bool
    ) -> None:
        columns = ["*"]

        if add_row_index:
            row_number = expressions.Window(
                this=expressions.Anonymous(this="ROW_NUMBER"),
                order=expressions.Order(expressions=[expressions.Literal.number(1)]),
            )
            columns.append(
                expressions.alias_(row_number, alias=FB_ROW_INDEX_FOR_JOIN, quoted=True),
            )

        query = sql_to_string(
            expressions.select(*columns).from_(
                get_fully_qualified_table_name(self.observation_table.location.table_details.dict())
            ),
            source_type=session.source_type,
        )
        await session.register_table_with_query(request_table_name, query)


@dataclass
class FeatureQuery:
    """
    FeatureQuery represents a sql query that materializes a temporary table for a set of features
    """

    sql: str
    table_name: str
    feature_names: list[str]


@dataclass
class HistoricalFeatureQuerySet:
    """
    HistoricalFeatureQuerySet is a collection of FeatureQuery that materializes intermediate feature
    tables and a final query that joins them into one.
    """

    feature_queries: list[FeatureQuery]
    output_query: str

    async def execute(
        self,
        session: BaseSession,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> None:
        """
        Execute the feature queries to materialize historical features

        Parameters
        ----------
        session: BaseSession
            Session object
        progress_callback: Optional[Callable[[int, str], None]]
            Optional progress callback function
        """
        total_num_queries = len(self.feature_queries) + 1
        materialized_feature_table = []
        try:
            for i, feature_query in enumerate(self.feature_queries):
                await session.execute_query_long_running(feature_query.sql)
                materialized_feature_table.append(feature_query.table_name)
                if progress_callback:
                    progress_callback(
                        int(100 * (i + 1) / total_num_queries),
                        PROGRESS_MESSAGE_COMPUTING_FEATURES,
                    )

            await session.execute_query_long_running(self.output_query)
            if progress_callback:
                progress_callback(100, PROGRESS_MESSAGE_COMPUTING_FEATURES)

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
) -> Tuple[expressions.Select, list[str]]:
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
    Tuple[expressions.Select], list[str]
        Tuple of feature query syntax tree and the list of feature names
    """
    planner = FeatureExecutionPlanner(
        graph,
        serving_names_mapping=serving_names_mapping,
        source_type=source_type,
        is_online_serving=False,
        parent_serving_preparation=parent_serving_preparation,
    )
    plan = planner.generate_plan(nodes)

    historical_features_expr = plan.construct_combined_sql(
        request_table_name=request_table_name,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=request_table_columns,
    )
    feature_names = plan.feature_names
    return historical_features_expr, feature_names


def split_nodes(
    graph: QueryGraph,
    nodes: list[Node],
    num_features_per_query: int,
    is_tile_cache: bool = False,
) -> list[list[Node]]:
    """
    Split nodes into multiple lists, each containing at most `num_features_per_query` nodes. Nodes
    within the same group after splitting will be executed in the same query.

    Parameters
    ----------
    graph: QueryGraph
        Query graph
    nodes : list[Node]
        List of nodes
    num_features_per_query : int
        Number of features per query
    is_tile_cache : bool
        Whether the output will be used for tile cache queries

    Returns
    -------
    list[list[Node]]
    """
    planner = FeatureExecutionPlanner(graph=graph, is_online_serving=False)

    def get_sort_key(node: Node) -> str:
        mapped_node = planner.graph.get_node_by_name(planner.node_name_map[node.name])
        agg_specs = planner.get_aggregation_specs(mapped_node)
        agg_spec = agg_specs[0]

        parts = [agg_spec.aggregation_type.value]
        if isinstance(agg_spec, TileBasedAggregationSpec):
            if is_tile_cache:
                # Tile cache queries joins with entity tracker tables. These tables are organized by
                # aggregation_id. The split should be random across different aggregation_id.
                parts.append(agg_spec.aggregation_id)
            else:
                # Tile based aggregation joins with tile tables. Sort by tile_table_id first to
                # group nodes that join with the same tile table.
                parts.extend([agg_spec.tile_table_id, agg_spec.aggregation_id])
        else:
            assert isinstance(agg_spec, NonTileBasedAggregationSpec)
            # These queries join with source tables directly. Sort by query node name of the source
            # to group nodes that join with the same source table.
            query_node = planner.graph.get_node_by_name(agg_spec.aggregation_source.query_node_name)
            parts.append(query_node.name)

        key = ",".join(parts)
        return key

    result = []
    sorted_nodes = sorted(nodes, key=get_sort_key)
    for i in range(0, len(sorted_nodes), num_features_per_query):
        current_nodes = sorted_nodes[i : i + num_features_per_query]
        result.append(current_nodes)
    return result


def construct_join_feature_sets_query(
    feature_queries: list[FeatureQuery],
    output_feature_names: list[str],
    request_table_name: str,
    request_table_columns: list[str],
) -> expressions.Select:
    """
    Construct the SQL code that joins the results of intermediate feature queries

    Parameters
    ----------
    feature_queries : list[FeatureQuery]
        List of feature queries
    output_feature_names : list[str]
        List of output feature names
    request_table_name : str
        Name of request table
    request_table_columns : list[str]
        List of column names in the request table. This should exclude the FB_ROW_INDEX_FOR_JOIN
        column which is only used for joining.

    Returns
    -------
    expressions.Select
    """
    expr = expressions.select(
        *(get_qualified_column_identifier(col, "REQ") for col in request_table_columns)
    ).from_(f"{request_table_name} AS REQ")

    table_alias_by_feature = {}
    for i, feature_set in enumerate(feature_queries):
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
        for feature_name in feature_set.feature_names:
            table_alias_by_feature[feature_name] = table_alias

    return expr.select(
        *[
            get_qualified_column_identifier(name, table_alias_by_feature[name])
            for name in output_feature_names
        ]
    )


def get_historical_features_query_set(  # pylint: disable=too-many-locals
    request_table_name: str,
    graph: QueryGraph,
    nodes: list[Node],
    request_table_columns: list[str],
    source_type: SourceType,
    output_table_details: TableDetails,
    output_feature_names: list[str],
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
        List of query graph nodes
    request_table_columns : list[str]
        List of column names in the training events
    source_type : SourceType
        Source type information
    output_table_details: TableDetails
        Output table details to write the results to
    output_feature_names : list[str]
        List of output feature names
    serving_names_mapping : dict[str, str] | None
        Optional mapping from original serving name to new serving name
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features

    Returns
    -------
    HistoricalFeatureQuerySet
    """
    # Process nodes in batches
    node_groups = split_nodes(graph, nodes, NUM_FEATURES_PER_QUERY)

    if len(node_groups) == 1:
        # Fallback to simpler non-batched query if there is only one group to avoid overhead
        sql_expr, _ = get_historical_features_expr(
            graph=graph,
            nodes=nodes,
            request_table_columns=request_table_columns,
            serving_names_mapping=serving_names_mapping,
            source_type=source_type,
            request_table_name=request_table_name,
            parent_serving_preparation=parent_serving_preparation,
        )
        output_query = sql_to_string(
            get_sql_adapter(source_type).create_table_as(
                table_details=output_table_details,
                select_expr=sql_expr,
            ),
            source_type=source_type,
        )
        return HistoricalFeatureQuerySet(feature_queries=[], output_query=output_query)

    feature_queries = []
    feature_set_table_name_prefix = f"__TEMP_{ObjectId()}"

    for i, nodes_group in enumerate(node_groups):
        feature_set_expr, feature_names = get_historical_features_expr(
            graph=graph,
            nodes=nodes_group,
            request_table_columns=[FB_ROW_INDEX_FOR_JOIN] + request_table_columns,
            serving_names_mapping=serving_names_mapping,
            source_type=source_type,
            request_table_name=request_table_name,
            parent_serving_preparation=parent_serving_preparation,
        )
        feature_set_table_name = f"{feature_set_table_name_prefix}_{i}"
        query = sql_to_string(
            get_sql_adapter(source_type).create_table_as(
                table_details=TableDetails(table_name=feature_set_table_name),
                select_expr=feature_set_expr,
            ),
            source_type,
        )
        feature_queries.append(
            FeatureQuery(
                sql=query,
                table_name=feature_set_table_name,
                feature_names=feature_names,
            )
        )
    output_expr = construct_join_feature_sets_query(
        feature_queries=feature_queries,
        output_feature_names=output_feature_names,
        request_table_name=request_table_name,
        request_table_columns=request_table_columns,
    )
    output_query = sql_to_string(
        get_sql_adapter(source_type).create_table_as(
            table_details=output_table_details,
            select_expr=output_expr,
        ),
        source_type=source_type,
    )
    return HistoricalFeatureQuerySet(feature_queries=feature_queries, output_query=output_query)


def get_feature_names(graph: QueryGraph, nodes: list[Node]) -> list[str]:
    """
    Get feature names given a list of ndoes

    Parameters
    ----------
    graph: QueryGraph
        Query graph
    nodes: list[Node]
        List of query graph node

    Returns
    -------
    list[str]
    """
    planner = FeatureExecutionPlanner(graph=graph, is_online_serving=False)
    return planner.generate_plan(nodes).feature_names
