"""
Utility functions for hashing nodes
"""

from typing import Any, Dict, Optional

from featurebyte.query_graph.enum import NodeType


def exclude_default_timestamp_metadata(node_parameters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Exclude default timestamp_metadata from node parameters. node_parameters is assumed to be the
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


def _exclude_timestamp_metadata_from_scd_base_parameters(
    node_parameters: Dict[str, Any], additional_params: Optional[list[str]] = None
) -> Dict[str, Any]:
    scd_base_parameters = ["end_timestamp_metadata", "effective_timestamp_metadata"]
    for scd_base_param in scd_base_parameters + (additional_params or []):
        if node_parameters.get(scd_base_param) is None:
            node_parameters.pop(scd_base_param, None)
    return node_parameters


def _exclude_timestamp_metadata_from_event_parameters(
    node_parameters: Dict[str, Any],
) -> Dict[str, Any]:
    if node_parameters.get("event_timestamp_metadata") is None:
        node_parameters.pop("event_timestamp_metadata", None)
    return node_parameters


def exclude_aggregation_and_lookup_node_timestamp_metadata(
    node_type: NodeType, node_parameters: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Exclude timestamp_metadata from aggregation node parameters.

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
        if node_parameters.get("timestamp_metadata", None) is None:
            node_parameters.pop("timestamp_metadata", None)

    if node_type in {NodeType.LOOKUP, NodeType.LOOKUP_TARGET}:
        if node_parameters.get("scd_parameters"):
            _exclude_timestamp_metadata_from_scd_base_parameters(node_parameters["scd_parameters"])

        if node_parameters.get("event_parameters"):
            _exclude_timestamp_metadata_from_event_parameters(node_parameters["event_parameters"])

    if node_type in {NodeType.AGGREGATE_AS_AT, NodeType.FORWARD_AGGREGATE_AS_AT}:
        _exclude_timestamp_metadata_from_scd_base_parameters(node_parameters)
    return node_parameters


def exclude_partition_metadata_from_node_parameters(
    node_parameters: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Exclude partition metadata from node parameters if it is None.

    Parameters
    ----------
    node_parameters: Dict[str, Any]
        Node parameters

    Returns
    -------
    Dict[str, Any]
    """
    if "columns" not in node_parameters:
        return node_parameters

    for column_spec in node_parameters["columns"]:
        column_spec.pop("partition_metadata", None)

    return node_parameters


def exclude_non_aggregation_with_timestamp_node_timestamp_metadata(
    node_type: NodeType, node_parameters: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Exclude timestamp_metadata from non aggregation with timestamp node parameters.

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
    if node_type == NodeType.JOIN:
        if node_parameters.get("scd_parameters"):
            _exclude_timestamp_metadata_from_scd_base_parameters(
                node_parameters["scd_parameters"],
                additional_params=["left_timestamp_metadata"],
            )
        if node_parameters.get("event_parameters"):
            _exclude_timestamp_metadata_from_event_parameters(node_parameters["event_parameters"])

    if node_type == NodeType.TRACK_CHANGES:
        if node_parameters.get("effective_timestamp_metadata") is None:
            node_parameters.pop("effective_timestamp_metadata", None)

    if node_type == NodeType.DT_EXTRACT:
        if node_parameters.get("timestamp_metadata") is None:
            node_parameters.pop("timestamp_metadata", None)

    if node_type == NodeType.DATE_DIFF:
        left_right_parameters = ["left_timestamp_metadata", "right_timestamp_metadata"]
        for param in left_right_parameters:
            if node_parameters.get(param) is None:
                node_parameters.pop(param, None)

    if node_type == NodeType.DATE_ADD:
        if node_parameters.get("left_timestamp_metadata") is None:
            node_parameters.pop("left_timestamp_metadata", None)

    if node_type == NodeType.INPUT:
        if node_parameters.get("event_timestamp_schema") is None:
            node_parameters.pop("event_timestamp_schema", None)

    return node_parameters


def handle_time_series_window_aggregate_node_parameters(node_parameters: Dict[str, Any]) -> None:
    """
    Handle time series window aggregate node parameters

    Parameters
    ----------
    node_parameters: Dict[str, Any]
        Node parameters
    """
    # Consider None blind_spot in CronFeatureJobSetting as the same as not provided for backward
    # compatibility
    feature_job_setting = node_parameters.get("feature_job_setting", {})
    if feature_job_setting.get("blind_spot") is None:
        feature_job_setting.pop("blind_spot", None)
