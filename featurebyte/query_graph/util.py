"""
Utility functions
"""
from typing import Any, Dict, List

import json

from featurebyte.query_graph.enum import NodeOutputType, NodeType


def hash_node(
    node_type: NodeType,
    node_params: Dict[str, Any],
    node_output_type: NodeOutputType,
    input_node_refs: List[int],
) -> int:
    """
    Hash the node related parameters for generating the node signature.

    Parameters
    ----------
    node_type: NodeType
        node type
    node_params: Dict
        node parameters
    node_output_type: NodeOutputType
        node output data type
    input_node_refs: List[int]
        input nodes hashed values

    Returns
    -------
    int

    """
    return hash(
        (
            node_type,
            json.dumps(node_params, sort_keys=True),
            node_output_type,
            tuple(input_node_refs),
        )
    )
