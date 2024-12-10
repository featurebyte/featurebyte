"""
Utility functions for hashing nodes
"""

from typing import Any, Dict


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
            if "timestamp_schema" in column_spec and column_spec["timestamp_schema"] is None:
                column_spec.pop("timestamp_schema")
    return node_parameters
