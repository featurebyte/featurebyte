"""
Utility functions for hashing nodes
"""

from typing import Any, Dict

from featurebyte.query_graph.enum import NodeType


def exclude_default_timestamp_schema(node_parameters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Exclude default timestamp_schema from node parameters. node_parameters is assumed to be the
    parameters from an InputNode.

    Parameters
    ----------
    node_parameters: Dict[str, Any]
        Node parameters

    Returns
    -------
    Dict[str, Any]
    """
    if "columns" in node_parameters:
        column_specs = node_parameters["columns"]
        for column_spec in column_specs:
            if "dtype_metadata" in column_spec and column_spec["dtype_metadata"] is None:
                column_spec.pop("dtype_metadata")
    return node_parameters


def exclude_aggregation_and_lookup_node_timestamp_schema(
    node_type: NodeType, node_parameters: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Exclude timestamp_schema from aggregation node parameters.

    Parameters
    ----------
    node_type: NodeType
        Node type
    node_parameters: Dict[str, Any]
        Node parameters

    Returns
    -------
    Dict[str, Any]
    """
    if node_type in {
        NodeType.GROUPBY,
        NodeType.NON_TILE_WINDOW_AGGREGATE,
        NodeType.FORWARD_AGGREGATE,
    }:
        if node_parameters.get("timestamp_schema", None) is None:
            node_parameters.pop("timestamp_schema", None)

    scd_parameters = ["end_timestamp_schema", "effective_timestamp_schema"]
    if node_type in {NodeType.LOOKUP, NodeType.LOOKUP_TARGET}:
        if node_parameters.get("scd_parameters"):
            for scd_parameter in scd_parameters:
                if node_parameters.get("scd_parameters").get(scd_parameter, None) is None:
                    node_parameters["scd_parameters"].pop(scd_parameter, None)

        if (
            node_parameters.get("event_parameters")
            and node_parameters.get("event_parameters").get("event_timestamp_schema", None) is None
        ):
            node_parameters["event_parameters"].pop("event_timestamp_schema", None)

    if node_type in {NodeType.AGGREGATE_AS_AT, NodeType.FORWARD_AGGREGATE_AS_AT}:
        for scd_parameter in scd_parameters:
            if node_parameters.get(scd_parameter) is None:
                node_parameters.pop(scd_parameter)

    return node_parameters
