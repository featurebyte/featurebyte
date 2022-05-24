"""
Utility functions
"""
from typing import Dict, Tuple

import json


def hash_node(node_type: str, node_params: Dict, input_node_refs: Tuple):
    return hash((node_type, json.dumps(node_params, sort_keys=True), input_node_refs))
