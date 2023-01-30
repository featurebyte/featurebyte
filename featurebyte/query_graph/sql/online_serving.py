"""
SQL generation for online serving
"""
from __future__ import annotations

from typing import List, Optional, Tuple, cast

from dataclasses import dataclass

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression, select

from featurebyte.enum import InternalName, SourceType, SpecialColumnName
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.adapter import BaseAdapter, get_sql_adapter
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME, quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.sql.online_serving_util import (
    get_online_store_table_name_from_entity_ids,
)
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec
from featurebyte.query_graph.sql.tile_util import calculate_first_and_last_tile_indices
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor


class OnlineStorePrecomputeQuery(FeatureByteBaseModel):
    """
    Represents a query required to support pre-computation for a feature. The pre-computed result is
    a single column to be stored in the online store table.

    sql: str
        Sql query that performs pre-computation for a feature. The result is not the feature values
        directly but intermediate results before post-aggregation transforms.
    tile_id: str
        Tile table identifier
    aggregation_id: str
        Aggregation identifier (identifies columns in the tile table)
    table_name: str
        Online store table name to store the pre-computed result
    result_name: str
        Column name generated by sql to be stored in online store table
    result_type: str
        Type of the colum generated by sql
    """

    sql: str
    tile_id: str
    aggregation_id: str
    table_name: str
    result_name: str
    result_type: str
    serving_names: List[str]


@dataclass
class OnlineStoreUniverse:
    """
    Represents a query that produces the "universe" of online store - the unique set of entities for
    a simple feature (derived from a single tile table) as at a point in time.

    It is derived based on the feature (e.g. a feature with larger window size will have a larger
    set of universe) and the underlying data (whether there are any events that occurred within the
    feature window as at a particular point in time).
    """

    expr: expressions.Select
    columns: List[str]


@dataclass
class PrecomputeQueryParams:
    """
    Information required to generate a OnlineStorePrecomputeQuery

    pruned_graph: QueryGraphModel
        Graph pruned to produce the aggregated column described by agg_spec
    pruned_node: Node
        Pruned node, see above
    agg_spec: TileBasedAggregationSpec
        Aggregation specification
    universe: OnlineStoreUniverse
        Query that produces the universe entities for the aggregation
    """

    pruned_graph: QueryGraphModel
    pruned_node: Node
    agg_spec: TileBasedAggregationSpec
    universe: OnlineStoreUniverse


class OnlineStorePrecomputePlan:
    """
    OnlineStorePrecomputePlan is responsible for extracting universe for online store feature
    pre-computation

    Parameters
    ----------
    graph: QueryGraph
        Query graph
    node: Node
        Query graph node
    adapter: BaseAdapter
        Instance of BaseAdapter for engine specific sql generation
    """

    def __init__(self, graph: QueryGraph, node: Node, adapter: BaseAdapter):
        self.adapter = adapter
        self.params_by_agg_result_name: dict[str, PrecomputeQueryParams] = {}
        self._update(graph, node)

    def construct_online_store_precompute_queries(
        self, source_type: SourceType
    ) -> list[OnlineStorePrecomputeQuery]:
        """
        Construct SQL queries for online store pre-computation

        Parameters
        ----------
        source_type: SourceType
            Source type information

        Returns
        -------
        list[OnlineStorePrecomputeQuery]
        """
        result = []
        for _, agg_params in self.params_by_agg_result_name.items():
            query = self._construct_online_store_precompute_query(
                params=agg_params,
                source_type=source_type,
            )
            result.append(query)
        return result

    def _get_first_and_last_indices(
        self, agg_spec: TileBasedAggregationSpec
    ) -> Tuple[Optional[Expression], Expression]:
        """
        Get the first and last tile indices required to compute the feature

        Parameters
        ----------
        agg_spec: TileBasedAggregationSpec
            Aggregation specification

        Returns
        -------
        Tuple[Optional[Expression], Expression]
        """
        point_in_time_expr = self._get_point_in_time_expr()
        window_size = agg_spec.window
        first_index, last_index = calculate_first_and_last_tile_indices(
            adapter=self.adapter,
            point_in_time_expr=point_in_time_expr,
            window_size=window_size,
            frequency=agg_spec.frequency,
            time_modulo_frequency=agg_spec.time_modulo_frequency,
        )
        return first_index, last_index

    def _construct_online_store_precompute_query(
        self,
        params: PrecomputeQueryParams,
        source_type: SourceType,
    ) -> OnlineStorePrecomputeQuery:

        planner = FeatureExecutionPlanner(
            params.pruned_graph,
            source_type=source_type,
            is_online_serving=False,
        )
        plan = planner.generate_plan([params.pruned_node])

        sql_expr = plan.construct_combined_sql(
            request_table_name=REQUEST_TABLE_NAME,
            point_in_time_column=SpecialColumnName.POINT_IN_TIME,
            request_table_columns=params.universe.columns,
            prior_cte_statements=[(REQUEST_TABLE_NAME, params.universe.expr)],
            exclude_post_aggregation=True,
        )
        sql = sql_to_string(sql_expr, source_type)
        table_name = get_online_store_table_name_from_entity_ids(set(params.agg_spec.entity_ids))

        op_struct = (
            OperationStructureExtractor(graph=params.pruned_graph)
            .extract(node=params.pruned_node)
            .operation_structure_map[params.pruned_node.name]
        )
        assert len(op_struct.aggregations) == 1
        aggregation = op_struct.aggregations[0]
        result_type = self.adapter.get_physical_type_from_dtype(aggregation.dtype)

        return OnlineStorePrecomputeQuery(
            sql=sql,
            tile_id=params.agg_spec.tile_table_id,
            aggregation_id=params.agg_spec.aggregation_id,
            table_name=table_name,
            result_name=params.agg_spec.agg_result_name,
            result_type=result_type,
            serving_names=sorted(params.agg_spec.serving_names),
        )

    def _construct_online_store_universe(
        self,
        agg_spec: TileBasedAggregationSpec,
    ) -> OnlineStoreUniverse:
        """
        Construct SQL expression that extracts the universe for online store. The result of this SQL
        contains a point in time column, so it can be used directly as the request table.

        Parameters
        ----------
        agg_spec: TileBasedAggregationSpec
            Aggregation specification

        Returns
        -------
        OnlineStoreUniverse
        """
        first_index, last_index = self._get_first_and_last_indices(agg_spec)

        tile_id = agg_spec.tile_table_id
        serving_names = agg_spec.serving_names
        keys = agg_spec.keys

        filter_conditions: list[Expression] = []
        if first_index is not None:
            filter_conditions.append(expressions.GTE(this="INDEX", expression=first_index))
        filter_conditions.append(expressions.LT(this="INDEX", expression=last_index))

        expr = (
            select(
                expressions.alias_(self._get_point_in_time_expr(), SpecialColumnName.POINT_IN_TIME),
                *[
                    expressions.alias_(
                        quoted_identifier(key_col), quoted_identifier(serving_name_col)
                    )
                    for key_col, serving_name_col in zip(keys, serving_names)
                ],
            )
            .distinct()
            .from_(tile_id)
            .where(expressions.and_(*filter_conditions))
        )
        universe_columns = [SpecialColumnName.POINT_IN_TIME.value] + serving_names

        return OnlineStoreUniverse(expr=expr, columns=universe_columns)

    def _update(self, graph: QueryGraph, node: Node) -> None:
        """
        Update state given a query graph node

        Parameters
        ----------
        graph: QueryGraph
            Query graph
        node: Node
            Query graph node
        """
        groupby_nodes = list(graph.iterate_nodes(node, NodeType.GROUPBY))
        for groupby_node in groupby_nodes:
            agg_specs = TileBasedAggregationSpec.from_groupby_query_node(groupby_node, self.adapter)
            for agg_spec in agg_specs:
                universe = self._construct_online_store_universe(agg_spec)
                project_node = graph.add_operation(
                    NodeType.PROJECT,
                    node_params={"columns": [agg_spec.feature_name]},
                    node_output_type=NodeOutputType.SERIES,
                    input_nodes=[groupby_node],
                )
                pruned_graph, node_name_map = graph.prune(project_node, aggressive=True)
                pruned_node = pruned_graph.get_node_by_name(node_name_map[project_node.name])
                self.params_by_agg_result_name[agg_spec.agg_result_name] = PrecomputeQueryParams(
                    pruned_graph=pruned_graph,
                    pruned_node=pruned_node,
                    agg_spec=agg_spec,
                    universe=universe,
                )

    @classmethod
    def _get_point_in_time_expr(cls) -> Expression:
        return cast(
            Expression,
            parse_one(f"CAST({InternalName.POINT_IN_TIME_SQL_PLACEHOLDER} AS TIMESTAMP)"),
        )


def get_online_store_precompute_queries(
    graph: QueryGraph,
    node: Node,
    source_type: SourceType,
) -> list[OnlineStorePrecomputeQuery]:
    """
    Construct the SQL code that can be scheduled for online store feature pre-computation

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    node : Node
        Query graph node
    source_type : SourceType
        Source type information

    Returns
    -------
    str
    """
    universe_plan = OnlineStorePrecomputePlan(graph, node, adapter=get_sql_adapter(source_type))
    queries = universe_plan.construct_online_store_precompute_queries(source_type=source_type)
    return queries


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
    op_struct = graph.extract_operation_structure(node)
    if not op_struct.is_time_based:
        return False
    has_point_in_time_groupby = False
    for _ in graph.iterate_nodes(node, NodeType.GROUPBY):
        has_point_in_time_groupby = True
    return has_point_in_time_groupby


def get_online_store_retrieval_sql(
    graph: QueryGraph,
    nodes: list[Node],
    source_type: SourceType,
    request_table_columns: list[str],
    request_table_name: Optional[str] = None,
    request_table_expr: Optional[expressions.Select] = None,
) -> str:
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

    Returns
    -------
    str
    """
    planner = FeatureExecutionPlanner(graph, source_type=source_type, is_online_serving=True)
    plan = planner.generate_plan(nodes)

    # Form a request table as a common table expression (CTE) and add the point in time column
    expr = select(*[f"REQ.{quoted_identifier(col).sql()}" for col in request_table_columns])
    expr = expr.select(f"SYSDATE() AS {SpecialColumnName.POINT_IN_TIME}")
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

    expr = plan.construct_combined_sql(
        request_table_name=request_table_name,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=request_table_columns,
        prior_cte_statements=ctes,
        exclude_columns={SpecialColumnName.POINT_IN_TIME},
    )

    return sql_to_string(expr, source_type=source_type)
