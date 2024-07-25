"""
Utility functions for models
"""

from datetime import datetime
from enum import Enum
from typing import Any

from bson import ObjectId


def serialize_obj(obj: Any) -> Any:
    """
    Serialize object

    Parameters
    ----------
    obj: Any
        Object to serialize

    Returns
    -------
    Any
    """
    if isinstance(obj, dict):
        return {k: serialize_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [serialize_obj(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    return obj
