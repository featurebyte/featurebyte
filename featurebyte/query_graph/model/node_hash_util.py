"""
Utility functions for hashing nodes
"""

from typing import Any, Dict, Optional

from featurebyte.query_graph.enum import NodeType


def exclude_default_column_spec_metadata(node_parameters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Exclude default metadata fields from node parameters such as dtype_metadata to maintain node
    hashes for existing graphs. node_parameters is assumed to be the parameters from an InputNode.

    Parameters
    ----------
    node_parameters: Dict[str, Any]
        Node parameters

    Returns
    -------
    Dict[str, Any]
    """
    fields_to_check = ["dtype_metadata", "nested_field_metadata"]
    if "columns" in node_parameters:
        column_specs = node_parameters["columns"]
        for column_spec in column_specs:
            for field in fields_to_check:
                if field in column_spec and column_spec[field] is None:
                    column_spec.pop(field)
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


def _exclude_snapshots_datetime_join_keys_from_parameters(
    node_parameters: Dict[str, Any],
) -> Dict[str, Any]:
    if node_parameters.get("snapshots_datetime_join_keys") is None:
        node_parameters.pop("snapshots_datetime_join_keys", None)
    return node_parameters


def _exclude_snapshots_parameters_from_asat_parameters(
    node_parameters: Dict[str, Any],
) -> Dict[str, Any]:
    if node_parameters.get("snapshots_parameters") is None:
        node_parameters.pop("snapshots_parameters", None)
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
        _exclude_snapshots_parameters_from_asat_parameters(node_parameters)
    return node_parameters


def exclude_partition_metadata_from_node_parameters(
    node_parameters: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Exclude partition metadata from node parameters.

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
        if node_parameters.get("snapshots_datetime_join_keys") is None:
            node_parameters.pop("snapshots_datetime_join_keys", None)

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


def handle_time_series_input_node_parameters(node_parameters: Dict[str, Any]) -> None:
    """
    Handle time series input node parameters for hash computation.

    Excludes ``id_columns`` from the hash when it is None or when it equals ``[id_column]``
    (the auto-populated single-column case), to maintain backward-compatible hashes for existing
    features.

    Parameters
    ----------
    node_parameters: Dict[str, Any]
        Node parameters
    """
    id_columns = node_parameters.get("id_columns")
    id_column = node_parameters.get("id_column")
    single_col_equivalent = [id_column] if id_column is not None else None
    if id_columns is None or id_columns == single_col_equivalent:
        node_parameters.pop("id_columns", None)


def _normalize_plural_to_singular(
    node_parameters: Dict[str, Any],
    plural_key: str,
    singular_key: str,
) -> None:
    """
    Normalize a plural list field to its singular equivalent for backward-compatible hash computation.

    - None → pop the plural key (field didn't exist in old features)
    - Single-item list → pop the plural key and write the single value into the singular key,
      so the hash matches old single-value features
    - Multi-item list → leave as-is (genuinely composite, new hash territory)

    Parameters
    ----------
    node_parameters: Dict[str, Any]
        Node parameters (mutated in place)
    plural_key: str
        The new list-valued field name (e.g. "entity_columns", "serving_names")
    singular_key: str
        The legacy scalar field name (e.g. "entity_column", "serving_name")
    """
    values = node_parameters.get(plural_key)
    if values is None:
        node_parameters.pop(plural_key, None)
    elif len(values) == 1:
        node_parameters.pop(plural_key)
        node_parameters[singular_key] = values[0]


def handle_lookup_node_parameters(node_parameters: Dict[str, Any]) -> None:
    """
    Handle lookup node parameters

    Parameters
    ----------
    node_parameters: Dict[str, Any]
        Node parameters
    """
    # Consider None snapshot_parameters in LookupParameters as the same as not provided for backward
    # compatibility
    if node_parameters.get("snapshots_parameters") is None:
        node_parameters.pop("snapshots_parameters", None)
    _normalize_plural_to_singular(node_parameters, "entity_columns", "entity_column")
    _normalize_plural_to_singular(node_parameters, "serving_names", "serving_name")
    _normalize_plural_to_singular(node_parameters, "entity_ids", "entity_id")
