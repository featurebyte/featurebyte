"""
Utilities related to online serving
"""

from __future__ import annotations

import hashlib
import json

from bson import ObjectId

from featurebyte.enum import InternalName


def get_online_store_table_name(entity_ids_set: set[ObjectId], result_type: str) -> str:
    """
    Get the online store table name given the entity ids and result type. Aggregations with the same
    set of entity and result type will be stored in the same table.

    Parameters
    ----------
    entity_ids_set : set[ObjectId]
        Entity ids
    result_type : str
        Column type of the aggregation result

    Returns
    -------
    str
    """
    hasher = hashlib.shake_128()
    hasher.update(json.dumps(sorted(map(str, entity_ids_set))).encode("utf-8"))
    hasher.update(result_type.encode("utf-8"))
    identifier = hasher.hexdigest(20)
    online_store_table_name = f"online_store_{identifier}".upper()
    return online_store_table_name


def get_version_placeholder(aggregation_result_name: str) -> str:
    """
    Get the version placeholder for the given aggregation result name

    Parameters
    ----------
    aggregation_result_name : str
        Aggregation result name

    Returns
    -------
    str
    """
    return aggregation_result_name + InternalName.ONLINE_STORE_VERSION_PLACEHOLDER_SUFFIX
