"""
Helpers to split a QueryGraph and nodes into smaller batches
"""

from __future__ import annotations

import hashlib
import json
import os

from sqlglot import expressions

from featurebyte.enum import InternalName
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner, FeatureQuery
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.specs import NonTileBasedAggregationSpec, TileBasedAggregationSpec
from featurebyte.query_graph.sql.tile_compute_combine import get_tile_compute_spec_signature

NUM_FEATURES_PER_QUERY = int(os.getenv("FEATUREBYTE_NUM_FEATURES_PER_QUERY", "20"))


def split_nodes(
    graph: QueryGraph,
    nodes: list[Node],
    num_features_per_query: int,
    source_info: SourceInfo,
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
    source_info: SourceInfo
        Source information

    Returns
    -------
    list[list[Node]]
    """
    if len(nodes) <= num_features_per_query:
        return [nodes]

    planner = FeatureExecutionPlanner(graph=graph, is_online_serving=False)
    interpreter = GraphInterpreter(graph, source_info)

    tile_compute_signature_mapping: dict[str, str] = {}

    def get_sort_key(node: Node) -> str:
        mapped_node = planner.graph.get_node_by_name(planner.node_name_map[node.name])
        agg_specs = planner.get_aggregation_specs(mapped_node)
        agg_spec = agg_specs[0]
        primary_table_ids = [
            input_node.parameters.id
            for input_node in QueryGraph.get_primary_input_nodes_from_graph_model(
                graph=planner.graph, node_name=mapped_node.name
            )
        ]
        primary_table_ids_key = ",".join([str(table_id) for table_id in primary_table_ids])
        parts = [primary_table_ids_key, agg_spec.aggregation_type.value]
        if isinstance(agg_spec, TileBasedAggregationSpec):
            aggregation_id = agg_spec.aggregation_id
            if aggregation_id not in tile_compute_signature_mapping:
                tile_infos = interpreter.construct_tile_gen_sql(node, False)
                tile_compute_signature = get_tile_compute_spec_signature(
                    tile_infos[0].tile_compute_spec
                )
                hasher = hashlib.shake_128()
                hasher.update(json.dumps(tile_compute_signature, sort_keys=True).encode("utf-8"))
                tile_compute_signature_mapping[aggregation_id] = hasher.hexdigest(20)
            parts.append(tile_compute_signature_mapping[aggregation_id])
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
    output_include_row_index: bool,
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
        List of column names in the request table. This should exclude the TABLE_ROW_INDEX
        column which is only used for joining.
    output_include_row_index: bool
        Whether to include the TABLE_ROW_INDEX column in the output

    Returns
    -------
    expressions.Select
    """
    expr = expressions.select(
        *(
            get_qualified_column_identifier(col, "REQ")
            for col in maybe_add_row_index_column(request_table_columns, output_include_row_index)
        )
    ).from_(expressions.Table(this=quoted_identifier(request_table_name), alias="REQ"))

    table_alias_by_feature = {}
    for i, feature_set in enumerate(feature_queries):
        table_alias = f"T{i}"
        expr = expr.join(
            expressions.Table(
                this=quoted_identifier(feature_set.feature_table_query.table_name),
                alias=expressions.TableAlias(this=expressions.Identifier(this=table_alias)),
            ),
            join_type="left",
            on=expressions.EQ(
                this=get_qualified_column_identifier(InternalName.TABLE_ROW_INDEX, "REQ"),
                expression=get_qualified_column_identifier(
                    InternalName.TABLE_ROW_INDEX, table_alias
                ),
            ),
        )
        for feature_name in feature_set.feature_names:
            table_alias_by_feature[feature_name] = table_alias

    # Select the output columns based on ordering defined in output_feature_names. A feature name
    # might not exist in table_alias_by_feature if it is not part of any successful feature queries.
    return expr.select(*[
        get_qualified_column_identifier(name, table_alias_by_feature[name])
        for name in output_feature_names
        if name in table_alias_by_feature
    ])


def maybe_add_row_index_column(
    request_table_columns: list[str], to_include_row_index_column: bool
) -> list[str]:
    """
    Helper function to add table row index column to a list of columns name when needed

    Parameters
    ----------
    request_table_columns: list[str]
        List of column names
    to_include_row_index_column: bool
        Whether to include the TABLE_ROW_INDEX column in the output

    Returns
    -------
    list[str]
    """
    if to_include_row_index_column:
        return [InternalName.TABLE_ROW_INDEX.value] + request_table_columns
    return request_table_columns[:]


def get_feature_names(graph: QueryGraph, nodes: list[Node]) -> list[str]:
    """
    Get feature names given a list of nodes

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
