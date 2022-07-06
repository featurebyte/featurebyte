"""
This module contains the enums used in routes directory
"""
from __future__ import annotations

from enum import Enum


class CollectionName(str, Enum):
    """
    Collection name
    """

    EVENT_DATA = "event_data"
    ENTITY = "entity"
