from typing import Any, Optional

import logging
import time

import pandas as pd
from sqlglot import select

from featurebyte.query_graph.feature_common import REQUEST_TABLE_NAME, prettify_sql
from featurebyte.query_graph.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.query_graph.tile_compute import construct_on_demand_tile_ctes

logger = logging.getLogger("featurebyte")


def construct_preview_request_table_sql(
    request_dataframe: pd.DataFrame, date_cols: list[str]
) -> str:
    """Construct a SELECT statement that uploads the request data

    This does not use write_pandas and should only be used for small request data (e.g. request data
    during preview that has only one row)

    Parameters
    ----------
    request_dataframe : DataFrame
        Request dataframe
    date_cols : list[str]
        List of date columns

    Returns
    -------
    str
    """
    row_sqls = []
    for _, row in request_dataframe.iterrows():
        columns = []
        for col, value in row.items():
            if col in date_cols:
                expr = f"CAST('{str(value)}' AS TIMESTAMP)"
            else:
                if isinstance(value, str):
                    expr = f"'{value}'"
                else:
                    expr = value
            columns.append(f"{expr} AS {col}")
        row_sql = select(*columns).sql()
        row_sqls.append(row_sql)
    return prettify_sql(" UNION ALL\n".join(row_sqls))


def get_feature_preview_sql(
    graph: QueryGraph,
    node: Node,
    entity_columns: Optional[list[str]],
    point_in_time_and_entity_id: dict[str, Any],
) -> str:
    """Get SQL code for previewing SQL

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    node : Node
        Query graph node
    entity_columns : Optional[list[str]]
        Entity columns
    point_in_time_and_entity_id : dict
        Preview request consisting of point in time and entity ID(s)

    Returns
    -------
    str

    Raises
    ------
    KeyError
        If any required entity columns is not provided
    """

    point_in_time = point_in_time_and_entity_id["POINT_IN_TIME"]
    if entity_columns is not None:
        for col in entity_columns:
            if col not in point_in_time_and_entity_id:
                raise KeyError(f"Entity column not provided: {col}")

    cte_statements = []

    planner = FeatureExecutionPlanner(graph)
    execution_plan = planner.generate_plan(node)

    # build required tiles
    logger.debug("Building required tiles")
    tic = time.time()
    on_demand_tile_ctes = construct_on_demand_tile_ctes(graph, node, point_in_time)
    cte_statements.extend(on_demand_tile_ctes)
    elapsed = time.time() - tic
    logger.debug(f"Building required tiles took {elapsed:.2}s")

    # prepare request table
    logger.debug("Uploading request table")
    tic = time.time()
    df_request = pd.DataFrame([point_in_time_and_entity_id])
    request_table_sql = construct_preview_request_table_sql(df_request, ["POINT_IN_TIME"])
    cte_statements.append((REQUEST_TABLE_NAME, request_table_sql))
    elapsed = time.time() - tic
    logger.debug(f"Uploading request table took {elapsed:.2}s")

    logger.debug("Generating full feature SQL")
    tic = time.time()
    preview_sql = execution_plan.construct_combined_sql(
        point_in_time_column="POINT_IN_TIME",
        request_table_columns=df_request.columns.tolist(),
        prior_cte_statements=cte_statements,
    )
    elapsed = time.time() - tic
    logger.debug(f"Generating full SQL took {elapsed:.2}s")
    logger.debug(f"Feature SQL:\n{preview_sql}")

    return preview_sql
