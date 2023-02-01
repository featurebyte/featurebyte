"""
Utilities related to online serving
"""
from __future__ import annotations

import hashlib
import json

from bson import ObjectId


def get_online_store_table_name_from_entity_ids(entity_ids_set: set[ObjectId]) -> str:
    """
    Get the online store table name given the entity ids

    Parameters
    ----------
    entity_ids_set : set[ObjectId]
        Entity ids

    Returns
    -------
    str
    """
    hasher = hashlib.shake_128()
    hasher.update(json.dumps(sorted(map(str, entity_ids_set))).encode("utf-8"))
    identifier = hasher.hexdigest(20)
    online_store_table_name = f"online_store_{identifier}"
    return online_store_table_name
