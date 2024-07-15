"""
SQL generation for online serving
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union, cast

import time
from datetime import datetime

import pandas as pd
from bson import ObjectId
from sqlglot import expressions
from sqlglot.expressions import Expression, select

from featurebyte.common.utils import prepare_dataframe_for_json
from featurebyte.enum import InternalName, SourceType, SpecialColumnName
from featurebyte.logging import get_logger
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.feature_query_set import FeatureQuery, FeatureQuerySet
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.batch_helper import (
    NUM_FEATURES_PER_QUERY,
    construct_join_feature_sets_query,
    get_feature_names,
    maybe_add_row_index_column,
    split_nodes,
)
from featurebyte.query_graph.sql.common import (
    REQUEST_TABLE_NAME,
    get_fully_qualified_table_name,
    get_qualified_column_identifier,
)
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.entity import (
    DUMMY_ENTITY_COLUMN_NAME,
    DUMMY_ENTITY_VALUE,
    get_combined_serving_names,
    get_combined_serving_names_expr,
)
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.sql.online_serving_util import get_version_placeholder
from featurebyte.query_graph.sql.template import SqlExpressionTemplate
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.session.base import BaseSession
from featurebyte.session.session_helper import execute_feature_query_set

logger = get_logger(__name__)

PROGRESS_MESSAGE_COMPUTING_ONLINE_FEATURES = "Computing online features"


def get_aggregation_result_names(
    graph: QueryGraph, nodes: list[Node], source_type: SourceType
) -> list[str]:
    """
    Get a list of aggregation result names that correspond to the graph and nodes

    Parameters
    ----------
    graph: QueryGraph
        Query graph
    nodes: list[Node]
        List of query graph nodes
    source_type: SourceType
        Source type information

    Returns
    -------
    list[str]
    """
    planner = FeatureExecutionPlanner(
        graph,
        source_type=source_type,
        is_online_serving=True,
    )
    plan = planner.generate_plan(nodes)
    return plan.tile_based_aggregation_result_names


def fill_version_placeholders(
    template_expr: Expression, versions: Dict[str, int]
) -> expressions.Select:
    """
    Fill the version placeholders in the SQL template

    Parameters
    ----------
    template_expr: Expression
        Retrieval query template with placeholders to be filled
    versions : Dict[str, int]
        Mapping from aggregation result name to version

    Returns
    -------
    expressions.Select
    """
    placeholders_mapping = {
        get_version_placeholder(agg_result_name): version
        for (agg_result_name, version) in versions.items()
    }
    return cast(
        expressions.Select,
        SqlExpressionTemplate(template_expr).render(placeholders_mapping, as_str=False),
    )


def fill_version_placeholders_for_query_set(
    query_set: FeatureQuerySet, versions: Dict[str, int]
) -> None:
    """
    Update an FeatureQuerySet in place to replace all feature version placeholders with concrete
    values

    Parameters
    ----------
    query_set: FeatureQuerySet
        FeatureQuerySet instance
    versions : Dict[str, int]
        Mapping from aggregation result name to version
    """
    for feature_query in query_set.feature_queries:
        assert isinstance(feature_query.sql, Expression)
        feature_query.sql = fill_version_placeholders(feature_query.sql, versions)
    assert isinstance(query_set.output_query, Expression)
    query_set.output_query = fill_version_placeholders(query_set.output_query, versions)


def construct_request_table_query(
    current_timestamp_expr: Expression,
    request_table_columns: list[str],
    request_table_expr: Optional[expressions.Select] = None,
    request_table_details: Optional[TableDetails] = None,
) -> expressions.Select:
    """
    Construct a Select expression for the request table

    Parameters
    ----------
    current_timestamp_expr: Expression
        The sql expression to use for the point-in-time value
    request_table_columns: list[str]
        Request table columns
    request_table_expr: Optional[expressions.Select]
        Select statement for the request table
    request_table_details: Optional[TableDetails]
        Location of the request table in the data warehouse

    Returns
    -------
    expressions.Select
    """
    expr = select(*[get_qualified_column_identifier(col, "REQ") for col in request_table_columns])
    expr = expr.select(
        expressions.alias_(current_timestamp_expr, alias=SpecialColumnName.POINT_IN_TIME)
    )
    request_table_columns.append(SpecialColumnName.POINT_IN_TIME)
    if request_table_expr is not None:
        expr = expr.from_(request_table_expr.subquery(alias="REQ"))
    else:
        assert request_table_details is not None
        expr = expr.from_(get_fully_qualified_table_name(request_table_details.dict(), alias="REQ"))
    return expr


def get_online_store_retrieval_expr(
    graph: QueryGraph,
    nodes: list[Node],
    source_type: SourceType,
    current_timestamp_expr: Expression,
    request_table_columns: list[str],
    request_table_expr: Optional[expressions.Select] = None,
    request_table_details: Optional[TableDetails] = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
) -> Tuple[expressions.Select, list[str]]:
    """
    Construct SQL code that can be used to lookup pre-computed features from online store

    Parameters
    ----------
    graph: QueryGraph
        Query graph
    nodes: list[Node]
        List of query graph nodes
    source_type: SourceType
        Source type information
    request_table_columns: list[str]
        Request table columns
    current_timestamp_expr: Expression
        The sql expression to use for the point-in-time value
    request_table_expr: Optional[expressions.Select]
        Select statement for the request table
    request_table_details: Optional[TableDetails]
        Location of the request table in the data warehouse
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features

    Returns
    -------
    expressions.Select
    """
    planner = FeatureExecutionPlanner(
        graph,
        source_type=source_type,
        is_online_serving=True,
        parent_serving_preparation=parent_serving_preparation,
    )
    plan = planner.generate_plan(nodes)

    # Form a request table as a common table expression (CTE) and add the point in time column
    request_query = construct_request_table_query(
        current_timestamp_expr=current_timestamp_expr,
        request_table_columns=request_table_columns,
        request_table_expr=request_table_expr,
        request_table_details=request_table_details,
    )
    request_table_name = "ONLINE_" + REQUEST_TABLE_NAME
    ctes = [(request_table_name, request_query)]

    output_expr = plan.construct_combined_sql(
        request_table_name=request_table_name,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=request_table_columns,
        prior_cte_statements=ctes,
        exclude_columns={SpecialColumnName.POINT_IN_TIME},
    )

    return output_expr, plan.feature_names


def get_current_timestamp_expr(
    request_timestamp: Optional[datetime], source_type: SourceType
) -> Expression:
    """
    Get the sql expression to use for the current timestamp

    Parameters
    ----------
    request_timestamp: Optional[datetime]
        The timestamp value to use as the point-in-time
    source_type: SourceType
        Source type information

    Returns
    -------
    Expression
    """
    adapter = get_sql_adapter(source_type)
    if request_timestamp is None:
        current_timestamp_expr = adapter.current_timestamp()
    else:
        current_timestamp_expr = make_literal_value(request_timestamp, cast_as_timestamp=True)
    return current_timestamp_expr


def add_concatenated_serving_names(
    select_expr: expressions.Select,
    concatenate_serving_names: Optional[list[str]],
    source_type: SourceType,
    serving_names_table_alias: Optional[str] = None,
) -> expressions.Select:
    """
    Add concatenated serving name column to the provided Select statement which is assumed to
    contain all the serving names.

    Parameters
    ----------
    select_expr: expressions.Select
        Select statement
    concatenate_serving_names: Optional[list[str]]
        List of serving names to concatenate
    source_type: SourceType
        Source type information
    serving_names_table_alias: Optional[str]
        Table alias for the serving names. Serving names will not be table qualified if not
        provided.

    Returns
    -------
    expressions.Select
    """
    if concatenate_serving_names is None:
        return select_expr
    if len(concatenate_serving_names) > 1:
        updated_select_expr = select_expr.select(
            expressions.alias_(
                get_combined_serving_names_expr(
                    concatenate_serving_names, serving_names_table_alias=serving_names_table_alias
                ),
                alias=get_combined_serving_names(concatenate_serving_names),
                quoted=True,
            )
        )
    elif source_type == SourceType.DATABRICKS_UNITY and len(concatenate_serving_names) == 0:
        updated_select_expr = select_expr.select(
            expressions.alias_(
                make_literal_value(DUMMY_ENTITY_VALUE),
                alias=DUMMY_ENTITY_COLUMN_NAME,
                quoted=True,
            )
        )
    else:
        updated_select_expr = select_expr
    return updated_select_expr


def get_online_features_query_set(  # pylint: disable=too-many-arguments,too-many-locals
    graph: QueryGraph,
    node_groups: list[list[Node]],
    source_type: SourceType,
    request_table_columns: list[str],
    output_feature_names: list[str],
    request_table_name: Optional[str],
    request_table_expr: Optional[expressions.Select] = None,
    request_table_details: Optional[TableDetails] = None,
    request_timestamp: Optional[datetime] = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    output_table_details: Optional[TableDetails] = None,
    output_include_row_index: bool = False,
    concatenate_serving_names: Optional[list[str]] = None,
) -> FeatureQuerySet:
    """
    Construct a FeatureQuerySet object to compute the online features

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    node_groups : list[list[Node]]
        List of query graph nodes divided into batches
    request_table_columns : list[str]
        List of column names in the training events
    source_type : SourceType
        Source type information
    output_table_details: TableDetails
        Output table details to write the results to
    output_feature_names : list[str]
        List of output feature names
    output_include_row_index: bool
        Whether to include the TABLE_ROW_INDEX column in the output
    request_table_name: Optional[str]
        Name of the registered request table
    request_table_expr: Optional[expressions.Select]
        Sql expression for the request table
    request_table_details: Optional[TableDetails]
        Location of the request table in the data warehouse
    request_timestamp: Optional[datetime]
        The timestamp value to use as the point-in-time
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    concatenate_serving_names: Optional[list[str]]
        List of serving names to concatenate as a new column, if specified

    Returns
    -------
    FeatureQuerySet
    """
    current_timestamp_expr = get_current_timestamp_expr(request_timestamp, source_type)

    if len(node_groups) == 1:
        # Fallback to simpler non-batched query if there is only one group to avoid overhead
        sql_expr, _ = get_online_store_retrieval_expr(
            graph=graph,
            nodes=node_groups[0],
            current_timestamp_expr=current_timestamp_expr,
            request_table_columns=maybe_add_row_index_column(
                request_table_columns, output_include_row_index
            ),
            request_table_expr=request_table_expr,
            request_table_details=request_table_details,
            source_type=source_type,
            parent_serving_preparation=parent_serving_preparation,
        )
        sql_expr = add_concatenated_serving_names(sql_expr, concatenate_serving_names, source_type)
        if output_table_details is not None:
            output_query = get_sql_adapter(source_type).create_table_as(
                table_details=output_table_details,
                select_expr=sql_expr,
            )
        else:
            output_query = sql_expr
        return FeatureQuerySet(
            feature_queries=[],
            output_query=output_query,
            output_table_name=(
                output_table_details.table_name if output_table_details is not None else None
            ),
            progress_message=PROGRESS_MESSAGE_COMPUTING_ONLINE_FEATURES,
        )

    feature_queries = []
    feature_set_table_name_prefix = f"__TEMP_{ObjectId()}"

    for i, nodes_group in enumerate(node_groups):
        feature_set_expr, feature_names = get_online_store_retrieval_expr(
            graph=graph,
            nodes=nodes_group,
            current_timestamp_expr=current_timestamp_expr,
            request_table_columns=[InternalName.TABLE_ROW_INDEX.value] + request_table_columns,
            request_table_expr=request_table_expr,
            request_table_details=request_table_details,
            source_type=source_type,
            parent_serving_preparation=parent_serving_preparation,
        )
        feature_set_table_name = f"{feature_set_table_name_prefix}_{i}"
        feature_queries.append(
            FeatureQuery(
                sql=get_sql_adapter(source_type).create_table_as(
                    table_details=TableDetails(table_name=feature_set_table_name),
                    select_expr=feature_set_expr,
                ),
                table_name=feature_set_table_name,
                feature_names=feature_names,
            )
        )

    assert request_table_name is not None
    output_expr = construct_join_feature_sets_query(
        feature_queries=feature_queries,
        output_feature_names=output_feature_names,
        request_table_name=request_table_name,
        request_table_columns=request_table_columns,
        output_include_row_index=output_include_row_index,
    )
    output_expr = add_concatenated_serving_names(
        output_expr,
        concatenate_serving_names,
        source_type,
        serving_names_table_alias="REQ",
    )
    if output_table_details is not None:
        output_expr = get_sql_adapter(source_type).create_table_as(  # type: ignore[assignment]
            table_details=output_table_details,
            select_expr=output_expr,
        )
    return FeatureQuerySet(
        feature_queries=feature_queries,
        output_query=output_expr,
        output_table_name=(
            output_table_details.table_name if output_table_details is not None else None
        ),
        progress_message=PROGRESS_MESSAGE_COMPUTING_ONLINE_FEATURES,
    )


class TemporaryBatchRequestTable(FeatureByteBaseModel):
    """
    Temporary batch request table created manually without going through the standard table
    materialization task. Contains the essential information needed for batch features
    materialization.
    """

    column_names: List[str]
    table_details: TableDetails


async def get_online_features(  # pylint: disable=too-many-locals,too-many-branches
    session: BaseSession,
    graph: QueryGraph,
    nodes: list[Node],
    request_data: Union[pd.DataFrame, BatchRequestTableModel, TemporaryBatchRequestTable],
    source_type: SourceType,
    online_store_table_version_service: OnlineStoreTableVersionService,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    output_table_details: Optional[TableDetails] = None,
    request_timestamp: Optional[datetime] = None,
    concatenate_serving_names: Optional[list[str]] = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    Get online features

    Parameters
    ----------
    session: BaseSession
        Session to use for executing the query
    graph: QueryGraph
        Query graph
    nodes: list[Node]
        List of query graph nodes
    request_data: Union[pd.DataFrame, BatchRequestTableModel]
        Request data as a dataframe or a BatchRequestTableModel
    source_type: SourceType
        Source type information
    online_store_table_version_service: OnlineStoreTableVersionService
        Online store table version service
    parent_serving_preparation: Optional[ParentServingPreparation]
        Preparation required for serving parent features
    output_table_details: Optional[TableDetails]
        Optional output table details to write the results to. If this parameter is provided, the
        function will return None (intended to be used when handling asynchronous batch online feature requests).
    request_timestamp: Optional[datetime]
        Request timestamp to use if provided
    concatenate_serving_names: Optional[list[str]]
        List of serving names to concatenate as a new column, if specified

    Returns
    -------
    Optional[List[Dict[str, Any]]]
    """
    tic = time.time()

    # Process nodes in batches
    node_groups = split_nodes(graph, nodes, NUM_FEATURES_PER_QUERY)

    if isinstance(request_data, pd.DataFrame):
        request_table_columns = request_data.columns.tolist()
        request_data[InternalName.TABLE_ROW_INDEX] = range(request_data.shape[0])
        request_table_expr = construct_dataframe_sql_expr(request_data, date_cols=[])
        request_table_details = None
    else:
        request_table_expr = None
        if isinstance(request_data, BatchRequestTableModel):
            request_table_details = request_data.location.table_details
            request_table_columns = [col.name for col in request_data.columns_info]
        else:
            request_table_details = request_data.table_details
            request_table_columns = request_data.column_names[:]

    if len(node_groups) > 1:
        # If using multiple queries, FeatureQuerySet requires request table to be registered as a
        # table beforehand.
        if isinstance(request_data, pd.DataFrame):
            request_table_name = f"{REQUEST_TABLE_NAME}_{session.generate_session_unique_id()}"
            await session.register_table(request_table_name, request_data)
        else:
            assert request_table_details is not None
            request_table_name = request_table_details.table_name
    else:
        request_table_name = None

    try:
        aggregation_result_names = get_aggregation_result_names(graph, nodes, source_type)
        versions = await online_store_table_version_service.get_versions(aggregation_result_names)
        query_set = get_online_features_query_set(
            graph,
            node_groups,
            source_type=source_type,
            request_table_columns=request_table_columns,
            output_feature_names=get_feature_names(graph, nodes),
            request_table_name=request_table_name,
            request_table_expr=request_table_expr,
            request_table_details=request_table_details,
            parent_serving_preparation=parent_serving_preparation,
            request_timestamp=request_timestamp,
            output_table_details=output_table_details,
            output_include_row_index=request_table_details is None,
            concatenate_serving_names=concatenate_serving_names,
        )
        fill_version_placeholders_for_query_set(query_set, versions)
        logger.debug(f"OnlineServingService sql prep elapsed: {time.time() - tic:.6f}s")

        tic = time.time()
        df_features = await execute_feature_query_set(session, query_set)
    finally:
        if request_table_name is not None and request_table_details is None:
            await session.drop_table(
                table_name=request_table_name,
                schema_name=session.schema_name,
                database_name=session.database_name,
            )

    if output_table_details is None:
        assert df_features is not None
        assert isinstance(request_data, pd.DataFrame)
        df_features = df_features.sort_values(InternalName.TABLE_ROW_INDEX).drop(
            InternalName.TABLE_ROW_INDEX, axis=1
        )
        df_features.index = request_data.index
        features = []
        prepare_dataframe_for_json(df_features)
        for _, row in df_features.iterrows():
            features.append(row.to_dict())
        logger.debug(f"OnlineServingService sql execution elapsed: {time.time() - tic:.6f}s")
        return features

    logger.debug(f"OnlineServingService sql execution elapsed: {time.time() - tic:.6f}s")
    return None
