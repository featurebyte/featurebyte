"""
Utility functions
"""
from typing import Any, Dict, List

import hashlib
import json

from featurebyte.query_graph.enum import NodeOutputType, NodeType


def hash_node(
    node_type: NodeType,
    node_params: Dict[str, Any],
    node_output_type: NodeOutputType,
    input_node_refs: List[int],
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
    hash_result = hasher.hexdigest(20)
    return hash_result
