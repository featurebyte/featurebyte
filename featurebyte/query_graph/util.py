"""
Utility functions
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List

from bson import json_util

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting


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
    hash_data = json_util.dumps(
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


def hash_input_node_hashes(input_node_hashes: List[str]) -> str:
    """
    Hash the input node hashes for generating the input node(s) signature.

    Parameters
    ----------
    input_node_hashes: List[str]
        input node hashes

    Returns
    -------
    str
    """
    hasher = hashlib.shake_128()
    hash_data = json_util.dumps(tuple(input_node_hashes), sort_keys=True).encode("utf-8")
    hasher.update(hash_data)
    hash_result = hasher.hexdigest(20)
    return hash_result


def get_aggregation_identifier(transformations_hash: str, parameters: dict[str, Any]) -> str:
    """Get aggregation identifier that can be used to determine the column names in tile table

    Aggregation identifier is determined by the combination of:
    1) Entity columns
    2) Category column specified in groupby
    3) Feature job settings
    4) EventView transformations

    Technically aggregation identifier can only consider EventView transformations since factors 1)
    to 3) are already considered as part of tile table identifier. But for ease of troubleshooting
    and as an extra guardrail, aggregation identifier includes all of these factors. It is more
    specific than the tile table identifier.

    Parameters
    ----------
    transformations_hash : str
        A hash that uniquely identifies the applied view transformations
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
    fjs = FeatureJobSetting(**parameters["feature_job_setting"]).to_seconds()
    period = fjs["period"]
    offset = fjs["offset"]
    blind_spot = fjs["blind_spot"]
    hash_components.append((period, offset, blind_spot))

    # Readable prefix for troubleshooting
    prefix = f"{parameters['agg_func']}"

    # EventView transformations
    hash_components.append(transformations_hash)

    # Hash all the factors above as the tile table identifier
    hasher = hashlib.shake_128()
    hasher.update(json.dumps(hash_components, sort_keys=True).encode("utf-8"))

    # Ignore "too many positional arguments" for hexdigest(20), but that seems like a false alarm
    aggregation_identifier = "_".join([prefix, hasher.hexdigest(20)])
    return aggregation_identifier


def get_tile_table_identifier_v1(row_index_lineage_hash: str, parameters: dict[str, Any]) -> str:
    """Get tile table identifier that can be used as tile table name

    Tile table identifier is determined by the combination of:
    1) Row index lineage
    2) Entity columns
    3) Category column specified in groupby
    4) Feature job settings

    Parameters
    ----------
    row_index_lineage_hash : str
        A unique identifier based on the row index lineage. This hash takes into account the
        InputNode (raw table) and any filtering and / or join operations applied.
    parameters : dict[str, Any]
        Node parameters

    Returns
    -------
    str
    """
    # This should include factors that affect whether a tile table can be reused
    hash_components: list[Any] = []

    # Aggregation related parameters
    keys_params = [
        parameters["keys"],
    ]
    if parameters["value_by"] is not None:
        keys_params.append(parameters["value_by"])
    aggregation_setting = tuple(keys_params)
    hash_components.append(aggregation_setting)

    # Feature job settings
    fjs = FeatureJobSetting(**parameters["feature_job_setting"]).to_seconds()
    period = fjs["period"]
    offset = fjs["offset"]
    blind_spot = fjs["blind_spot"]
    hash_components.append((period, offset, blind_spot))

    # Row index lineage determines the rows that will be present in the tile table
    hash_components.append(row_index_lineage_hash)

    # Readable prefix for troubleshooting
    prefix = f"tile_f{period}_m{offset}_b{blind_spot}"

    # Hash all the factors above as the tile table identifier
    hasher = hashlib.shake_128()
    hasher.update(json.dumps(hash_components, sort_keys=True).encode("utf-8"))

    # Ignore "too many positional arguments" for hexdigest(20), but that seems like a false alarm
    tile_table_identifier = "_".join([prefix, hasher.hexdigest(20)])
    return tile_table_identifier.upper()


def get_tile_table_identifier_v2(transformations_hash: str, parameters: dict[str, Any]) -> str:
    """
    Get tile table identifier that can be used as tile table name

    In v2, different aggregations do not share the same tile table.

    Parameters
    ----------
    transformations_hash : str
        A hash that uniquely identifies the applied view transformations
    parameters : dict[str, Any]
        Node parameters

    Returns
    -------
    str
    """
    aggregation_id = get_aggregation_identifier(
        transformations_hash=transformations_hash, parameters=parameters
    )
    return f"TILE_{aggregation_id}".upper()


def append_to_lineage(lineage: tuple[str, ...], node_name: str) -> tuple[str, ...]:
    """
    Add operation node name to the (row-index) lineage (list of node names)

    Parameters
    ----------
    lineage: tuple[str, ...]
        tuple of node names to represent the feature/row-index lineage
    node_name: str
        operation node name

    Returns
    -------
    updated_lineage: tuple[str, ...]
        updated lineage after adding the new operation name

    """
    output = list(lineage)
    output.append(node_name)
    return tuple(output)


def sort_lists_by_first_list(by_list: List[Any], *lists: List[Any]) -> List[List[Any]]:
    """
    Sorts multiple lists by the first list

    Parameters
    ----------
    by_list: List[Any]
        list to sort by
    lists: List[Any]
        lists to sort

    Returns
    -------
    sorted_lists: List[List[Any]]
        sorted lists
    """
    sorted_lists = [list(t) for t in zip(*sorted(zip(by_list, *lists)))]
    return sorted_lists
