"""
SQL generation for online serving
"""
from __future__ import annotations

from typing import Any, List, Tuple, cast

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass

from bson import ObjectId
from sqlglot import Expression, expressions, parse_one, select

from featurebyte.enum import InternalName, SourceType, SpecialColumnName
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import AliasNode, ProjectNode
from featurebyte.query_graph.sql.adapter import BaseAdapter, get_sql_adapter
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME, quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner
from featurebyte.query_graph.sql.specs import PointInTimeAggregationSpec
from featurebyte.query_graph.sql.tile_util import calculate_first_and_last_tile_indices


class OnlineStoreUniversePlan:
    """
    OnlineStoreUniversePlan is responsible for extracting universe for online store feature
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
        self.max_window_size_by_tile_id: dict[str, int] = defaultdict(int)
        self.params_by_tile_id: dict[str, Any] = {}
        self._update(graph, node)

    def get_first_and_last_indices_by_tile_id(self) -> List[Tuple[str, Expression, Expression]]:
        """
        Get the first and last tile indices required to compute the feature

        Returns
        -------
        List[Tuple[str, Expression, Expression]]
        """
        out = []
        point_in_time_expr = self._get_point_in_time_expr()
        for tile_id, window_size in self.max_window_size_by_tile_id.items():
            params_dict = self.params_by_tile_id[tile_id]
            first_index, last_index = calculate_first_and_last_tile_indices(
                adapter=self.adapter,
                point_in_time_expr=point_in_time_expr,
                window_size=window_size,
                frequency=params_dict["frequency"],
                time_modulo_frequency=params_dict["time_modulo_frequency"],
            )
            out.append((tile_id, first_index, last_index))
        return out

    def construct_online_store_universe(self) -> Tuple[expressions.Select, List[str]]:
        """
        Construct SQL expression that extracts the universe for online store. The result of this SQL
        contains a point in time column, so it can be used directly as the request table.

        Returns
        -------
        Tuple[expressions.Select, List[str]]

        Raises
        ------
        NotImplementedError
            if feature is derived from multiple tile tables
        """

        first_and_last_indices_by_tile_id = self.get_first_and_last_indices_by_tile_id()

        # TODO: Handle features derived from multiple tile tables
        #
        # If there are more than one tile tables, the feature is a complex feature derived from
        # multiple features with entities that are identical or have a parent child relationship,
        # from the same EventData / ItemData. For the parent child relationship case, we need to
        # identify the least ancestral entity and use that to define the universe. For now, assert
        # that the feature is simple.
        if len(first_and_last_indices_by_tile_id) != 1:
            raise NotImplementedError()

        tile_id, first_index, last_index = first_and_last_indices_by_tile_id[0]
        params = self.params_by_tile_id[tile_id]
        serving_names = params["serving_names"]
        keys = params["keys"]

        filter_condition = expressions.and_(
            expressions.GTE(this="INDEX", expression=first_index),
            expressions.LT(this="INDEX", expression=last_index),
        )
        expr = (
            expressions.Select(distinct=True)
            .select(
                expressions.alias_(self._get_point_in_time_expr(), SpecialColumnName.POINT_IN_TIME),
                *[
                    expressions.alias_(
                        quoted_identifier(key_col), quoted_identifier(serving_name_col)
                    )
                    for key_col, serving_name_col in zip(keys, serving_names)
                ],
            )
            .from_(tile_id)
            .where(filter_condition)
        )
        universe_columns = [SpecialColumnName.POINT_IN_TIME] + serving_names

        return expr, universe_columns

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
            agg_specs = PointInTimeAggregationSpec.from_groupby_query_node(groupby_node)
            for agg_spec in agg_specs:
                tile_id = agg_spec.tile_table_id
                self.max_window_size_by_tile_id[tile_id] = max(
                    agg_spec.window, self.max_window_size_by_tile_id[tile_id]
                )
                self.params_by_tile_id[tile_id] = {
                    "keys": agg_spec.keys,
                    "serving_names": agg_spec.serving_names,
                    "frequency": agg_spec.frequency,
                    "time_modulo_frequency": agg_spec.time_modulo_frequency,
                }

    @classmethod
    def _get_point_in_time_expr(cls) -> Expression:
        return parse_one(f"CAST({InternalName.POINT_IN_TIME_SQL_PLACEHOLDER} AS TIMESTAMP)")


def get_online_store_feature_compute_sql(
    graph: QueryGraph,
    node: Node,
    source_type: SourceType,
) -> str:
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
    planner = FeatureExecutionPlanner(graph, source_type=source_type)
    plan = planner.generate_plan([node])

    universe_plan = OnlineStoreUniversePlan(graph, node, adapter=get_sql_adapter(source_type))
    universe_expr, universe_columns = universe_plan.construct_online_store_universe()

    sql_expr = plan.construct_combined_sql(
        request_table_name=REQUEST_TABLE_NAME,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=universe_columns,
        prior_cte_statements=[(REQUEST_TABLE_NAME, universe_expr)],
    )

    return sql_to_string(sql_expr, source_type)


def get_entities_ids_and_serving_names(
    graph: QueryGraph, node: Node
) -> Tuple[set[ObjectId], set[str]]:
    """
    Get the union of all entity ids of the node's input nodes. Only point in time groupby nodes are
    considered, since other nodes that produce features (e.g. ItemGroupyNode) generate features that
    cannot be pre-computed.

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    node : Node
        Query graph node

    Returns
    -------
    Tuple[set[ObjectId], set[str]]
    """
    entity_ids = set()
    serving_names = set()
    for groupby_node in graph.iterate_nodes(node, NodeType.GROUPBY):
        parameters = groupby_node.parameters.dict()
        entity_ids.update(parameters["entity_ids"])
        serving_names.update(parameters["serving_names"])
    return entity_ids, serving_names


def get_online_store_table_name_from_entity_ids(entity_ids_set: set[ObjectId]) -> str:
    """
    Get the online store table name given a query graph and node

    Parameters
    ----------
    entity_ids_set : set[ObjectId]
        Entity ids

    Returns
    -------
    str
    """
    hasher = hashlib.shake_128()
    hasher.update(json.dumps(sorted(map(str, entity_ids_set))).encode("utf-8"))
    identifier = hasher.hexdigest(20)
    online_store_table_name = f"online_store_{identifier}"
    return online_store_table_name


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
    has_point_in_time_groupby = False
    for _ in graph.iterate_nodes(node, NodeType.GROUPBY):
        has_point_in_time_groupby = True
    return has_point_in_time_groupby


@dataclass
class OnlineStoreLookupSpec:
    """
    OnlineStoreLookupSpec represents a feature that can be looked up from the online store
    """

    feature_name: str
    feature_store_table_name: str
    serving_names: list[str]

    @classmethod
    def from_graph_and_node(cls, graph: QueryGraph, node: Node) -> OnlineStoreLookupSpec:
        """
        Create an OnlineStoreLookupSpec from query graph node associated with a Feature

        Parameters
        ----------
        graph: QueryGraph
            Query graph
        node: Node
            Query graph node

        Returns
        -------
        OnlineStoreLookupSpec
        """
        entity_ids_set, serving_names_set = get_entities_ids_and_serving_names(graph, node)
        feature_store_table_name = get_online_store_table_name_from_entity_ids(entity_ids_set)

        # node should be associated with a Feature, so it must be either a ProjectNode or an
        # AliasNode
        assert isinstance(node, (ProjectNode, AliasNode))
        if isinstance(node, ProjectNode):
            feature_name = cast(str, node.parameters.columns[0])
        else:
            # node is an AliasNode
            feature_name = cast(str, node.parameters.name)

        spec = OnlineStoreLookupSpec(
            feature_name=feature_name,
            feature_store_table_name=feature_store_table_name,
            serving_names=sorted(serving_names_set),
        )
        return spec


class OnlineStoreRetrievePlan:
    """
    OnlineStoreRetrievePlan is responsible for generating SQL for feature lookup from online store
    """

    def __init__(self, graph: QueryGraph):
        self.graph = graph

        # Mapping from online store table name to OnlineStoreLookupSpec list
        self.online_store_specs: dict[str, list[OnlineStoreLookupSpec]] = defaultdict(list)

        # List of feature names in the order they are provided
        self.feature_names: list[str] = []

    def update_if_eligible(self, node: Node) -> bool:
        """
        Check if the node is eligible for online store lookup and if so update state

        Parameters
        ----------
        node: Node
            Query graph node

        Returns
        -------
        bool
        """
        if not is_online_store_eligible(self.graph, node):
            return False

        spec = OnlineStoreLookupSpec.from_graph_and_node(self.graph, node)
        self.online_store_specs[spec.feature_store_table_name].append(spec)
        self.feature_names.append(spec.feature_name)
        return True

    def construct_retrieval_sql(
        self,
        request_table_name: str,
        request_table_columns: list[str],
    ) -> expressions.Select:
        """
        Construct SQL expression to lookup feature from online store

        Parameters
        ----------
        request_table_name: str
            Name of the request table
        request_table_columns: list[str]
            Columns in the request table

        Returns
        -------
        expressions.Select
        """
        # Original columns
        expr = select(*[quoted_identifier(col) for col in request_table_columns]).from_(
            expressions.alias_(quoted_identifier(request_table_name), alias="REQ")
        )

        # Perform one left join for each unique online store table and retrieve one or more
        # pre-computed features
        qualified_feature_names = {}
        for index, (feature_store_table_name, specs) in enumerate(self.online_store_specs.items()):
            serving_names = specs[0].serving_names
            feature_names = [spec.feature_name for spec in specs]
            join_alias = f"T{index}"
            join_conditions = expressions.and_(
                *[
                    f"REQ.{quoted_identifier(name).sql()} = {join_alias}.{quoted_identifier(name).sql()}"
                    for name in serving_names
                ]
            )
            for feature_name in feature_names:
                qualified_feature_names[
                    feature_name
                ] = f"{join_alias}.{quoted_identifier(feature_name).sql()}"
            expr = expr.join(
                expressions.Identifier(this=feature_store_table_name),
                join_alias=join_alias,
                on=join_conditions,
                join_type="left",
            )

        # Select the joined features in their original order
        expr = expr.select(
            *[qualified_feature_names[feature_name] for feature_name in self.feature_names]
        )
        return expr


def construct_feature_sql_with_enriched_request_table(
    expr: expressions.Select,
    graph: QueryGraph,
    online_excluded_nodes: list[Node],
    request_table_name: str,
    enriched_request_table_columns: list[str],
    source_type: SourceType,
) -> expressions.Select:
    """
    Construct SQL expression to compute features on demand for features that are cannot be
    pre-computed in online store (e.g. non-time aware aggregation, SCD lookup,etc)

    Parameters
    ----------
    expr: Select
        Expression with the request table with features added from online store
    graph: QueryGraph
        Query graph
    online_excluded_nodes: list[Node]
        List of nodes corresponding to features not ava
    request_table_name: str
        Original request table name
    enriched_request_table_columns: list[str]
        Columns in the updated request table (original request table columns and features looked up
        from online store)
    source_type: SourceType
        Source type information

    Returns
    -------
    Select
    """
    planner = FeatureExecutionPlanner(graph, source_type=source_type)
    plan = planner.generate_plan(online_excluded_nodes)

    new_request_table_expr = expr.select(f"SYSDATE() AS {SpecialColumnName.POINT_IN_TIME}")
    new_request_table_name = request_table_name + "_POST_FEATURE_STORE_LOOKUP"
    ctes = [(new_request_table_name, new_request_table_expr)]

    expr = plan.construct_combined_sql(
        request_table_name=new_request_table_name,
        point_in_time_column=SpecialColumnName.POINT_IN_TIME,
        request_table_columns=enriched_request_table_columns,
        prior_cte_statements=ctes,
    )
    return expr


def get_online_store_retrieval_sql(
    request_table_name: str,
    request_table_columns: list[str],
    graph: QueryGraph,
    nodes: list[Node],
    source_type: SourceType,
) -> str:
    """
    Construct SQL code that can be used to lookup pre-computed features from online store

    Parameters
    ----------
    request_table_name: str
        Name of the request table
    request_table_columns: list[str]
        Request table columns
    graph: QueryGraph
        Query graph
    nodes: list[Node]
        List of query graph nodes
    source_type: SourceType
        Source type information

    Returns
    -------
    str
    """
    online_store_plan = OnlineStoreRetrievePlan(graph)
    online_excluded_nodes = []

    for node in nodes:
        if not online_store_plan.update_if_eligible(node):
            online_excluded_nodes.append(node)

    expr = online_store_plan.construct_retrieval_sql(
        request_table_name=request_table_name, request_table_columns=request_table_columns
    )

    if online_excluded_nodes:
        enriched_request_table_columns = request_table_columns + online_store_plan.feature_names
        expr = construct_feature_sql_with_enriched_request_table(
            expr=expr,
            graph=graph,
            online_excluded_nodes=online_excluded_nodes,
            request_table_name=request_table_name,
            enriched_request_table_columns=enriched_request_table_columns,
            source_type=source_type,
        )

    return sql_to_string(expr, source_type=source_type)
