"""
SQL generation for online serving
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union, cast

import time
from dataclasses import dataclass

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte.common.utils import prepare_dataframe_for_json
from featurebyte.enum import SourceType, SpecialColumnName
from featurebyte.logging import get_logger
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.parent_serving import ParentServingPreparation
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import (
    REQUEST_TABLE_NAME,
    get_fully_qualified_table_name,
    quoted_identifier,
    sql_to_string,
)
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.sql.online_serving_util import get_version_placeholder
from featurebyte.query_graph.sql.template import SqlExpressionTemplate
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


def is_online_store_eligible(graph: QueryGraph, node: Node) -> bool:
    """
    Check whether the feature represented by the given node is eligible for online store lookup

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    node : Node
        Query graph node

    Returns
    -------
    bool
    """
    op_struct = graph.extract_operation_structure(node, keep_all_source_columns=True)
    if not op_struct.is_time_based:
        return False
    has_point_in_time_groupby = False
    for _ in graph.iterate_nodes(node, NodeType.GROUPBY):
        has_point_in_time_groupby = True
    return has_point_in_time_groupby


@dataclass
class OnlineStoreRetrievalTemplate:
    """
    SQL code template for retrieving data from online store

    sql_template: SqlExpressionTemplate
        SQL code template with online store table version placeholders
    aggregation_result_names: list[str]
        Aggregation result names involved in the online serving query
    """

    sql_template: SqlExpressionTemplate
    aggregation_result_names: list[str]

    def fill_version_placeholders(self, versions: Dict[str, int]) -> expressions.Select:
        """
        Fill the version placeholders in the SQL template

        Parameters
        ----------
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
            expressions.Select, self.sql_template.render(placeholders_mapping, as_str=False)
        )


def get_online_store_retrieval_template(
    graph: QueryGraph,
    nodes: list[Node],
    source_type: SourceType,
    request_table_columns: list[str],
    request_table_name: Optional[str] = None,
    request_table_expr: Optional[expressions.Select] = None,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
) -> OnlineStoreRetrievalTemplate:
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
    request_table_name: Optional[str]
        Name of the request table
    request_table_expr: Optional[expressions.Select]
        Select statement for the request table
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
    expr = select(*[f"REQ.{quoted_identifier(col).sql()}" for col in request_table_columns])
    adapter = get_sql_adapter(source_type)
    expr = expr.select(
        expressions.alias_(adapter.current_timestamp(), alias=SpecialColumnName.POINT_IN_TIME)
    )
    request_table_columns.append(SpecialColumnName.POINT_IN_TIME)

    if request_table_name is not None:
        # Case 1: Request table is already registered as a table with a name
        expr = expr.from_(expressions.alias_(quoted_identifier(request_table_name), alias="REQ"))
    else:
        # Case 2: Request table is provided as an embedded query
        assert request_table_expr is not None
        expr = expr.from_(request_table_expr.subquery(alias="REQ"))
        request_table_name = REQUEST_TABLE_NAME

    request_table_name = "ONLINE_" + request_table_name
    ctes = [(request_table_name, expr)]

    output_expr = plan.construct_combined_sql(
        request_table_name=request_table_name,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=request_table_columns,
        prior_cte_statements=ctes,
        exclude_columns={SpecialColumnName.POINT_IN_TIME},
    )

    return OnlineStoreRetrievalTemplate(
        sql_template=SqlExpressionTemplate(output_expr, source_type),
        aggregation_result_names=plan.tile_based_aggregation_result_names,
    )


async def get_online_features(
    session: BaseSession,
    graph: QueryGraph,
    nodes: list[Node],
    request_data: Union[pd.DataFrame, BatchRequestTableModel],
    source_type: SourceType,
    online_store_table_version_service: OnlineStoreTableVersionService,
    parent_serving_preparation: Optional[ParentServingPreparation] = None,
    output_table_details: Optional[TableDetails] = None,
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

    Returns
    -------
    Optional[List[Dict[str, Any]]]
    """
    tic = time.time()

    if isinstance(request_data, pd.DataFrame):
        request_table_expr = construct_dataframe_sql_expr(request_data, date_cols=[])
        request_table_columns = request_data.columns.tolist()
    else:
        request_table_expr = expressions.select("*").from_(
            get_fully_qualified_table_name(request_data.location.table_details.dict())
        )
        request_table_columns = [col.name for col in request_data.columns_info]

    retrieval_template = get_online_store_retrieval_template(
        graph,
        nodes,
        source_type=source_type,
        request_table_columns=request_table_columns,
        request_table_expr=request_table_expr,
        parent_serving_preparation=parent_serving_preparation,
    )
    versions = await online_store_table_version_service.get_versions(
        retrieval_template.aggregation_result_names
    )
    retrieval_expr = retrieval_template.fill_version_placeholders(versions)
    logger.debug(f"OnlineServingService sql prep elapsed: {time.time() - tic:.6f}s")

    tic = time.time()
    if output_table_details is None:
        retrieval_sql = sql_to_string(retrieval_expr, source_type=source_type)
        df_features = await session.execute_query(retrieval_sql)
        assert df_features is not None

        features = []
        prepare_dataframe_for_json(df_features)
        for _, row in df_features.iterrows():
            features.append(row.to_dict())
        logger.debug(f"OnlineServingService sql execution elapsed: {time.time() - tic:.6f}s")
        return features

    # write the request to the output table
    expression = get_sql_adapter(session.source_type).create_table_as(
        table_details=output_table_details, select_expr=retrieval_expr
    )
    query = sql_to_string(expression, source_type=session.source_type)
    await session.execute_query_long_running(query)
    logger.debug(f"OnlineServingService sql execution elapsed: {time.time() - tic:.6f}s")
    return None
