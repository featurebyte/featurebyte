"""
Utility functions
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

import hashlib
import json

from featurebyte.query_graph.enum import NodeOutputType, NodeType

if TYPE_CHECKING:
    from featurebyte.query_graph.graph import Node, QueryGraph


def hash_node(
    node_type: NodeType,
    node_params: Dict[str, Any],
    node_output_type: NodeOutputType,
    input_node_refs: List[str],
) -> str:
    """
    Hash the node related parameters for generating the node signature.

    Parameters
    ----------
    node_type: NodeType
        node type
    node_params: Dict[str, Any]
        node parameters
    node_output_type: NodeOutputType
        node output data type
    input_node_refs: List[int]
        input nodes hashed values

    Returns
    -------
    str
    """
    hasher = hashlib.shake_128()
    hash_data = json.dumps(
        (
            node_type,
            node_params,
            node_output_type,
            tuple(input_node_refs),
        ),
        sort_keys=True,
    ).encode("utf-8")
    hasher.update(hash_data)
    hash_result = hasher.hexdigest(20)  # pylint: disable=E1121
    return hash_result


def get_tile_table_identifier(transformations_hash: str, parameters: dict[str, Any]) -> str:
    """Get tile table identifier that can be used as tile table name

    Parameters
    ----------
    transformations_hash : str
        Input node hash
    parameters : dict[str, Any]
        Node parameters

    Returns
    -------
    str
    """
    # This should include factors that affect whether a tile table can be reused
    hash_components: list[Any] = []

    # Aggregation related parameters
    aggregation_setting_lst = [
        parameters["keys"],
        parameters["parent"],
        parameters["agg_func"],
    ]
    if parameters["value_by"] is not None:
        aggregation_setting_lst.append(parameters["value_by"])
    aggregation_setting = tuple(aggregation_setting_lst)
    hash_components.append(aggregation_setting)

    # Feature job settings
    job_setting = (
        parameters["frequency"],
        parameters["time_modulo_frequency"],
        parameters["blind_spot"],
    )
    hash_components.append(job_setting)

    # Readable prefix for troubleshooting
    prefix = (
        f"{parameters['agg_func']}"
        f"_f{parameters['frequency']}"
        f"_m{parameters['time_modulo_frequency']}"
        f"_b{parameters['blind_spot']}"
    )

    # EventView transformations
    hash_components.append(transformations_hash)

    # Hash all the factors above as the tile table identifier
    hasher = hashlib.shake_128()
    hasher.update(json.dumps(hash_components, sort_keys=True).encode("utf-8"))

    # Ignore "too many positional arguments" for hexdigest(20), but that seems like a false alarm
    tile_table_identifier = "_".join([prefix, hasher.hexdigest(20)])  # pylint: disable=E1121
    return tile_table_identifier
