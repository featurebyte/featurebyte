"""
This module contains groupby related class
"""
from __future__ import annotations

from featurebyte.core.event_source import EventSource


class EventSourceGroupBy:
    """
    EventSourceGroupBy class
    """

    def __init__(self, obj: EventSource, keys: str | list[str]):
        assert isinstance(obj, EventSource)

        keys_value = []
        if isinstance(keys, str):
            keys_value.append(keys)
        elif isinstance(keys, list):
            keys_value = keys
        else:
            raise TypeError(f"Grouping {obj} by '{keys}' is not supported!")

        for key in keys_value:
            if key not in obj.columns:
                raise KeyError(f"Column '{key}' not found in {obj}!")

        self.obj = obj
        self.keys = keys_value

    def __repr__(self) -> str:
        return f"EventSourceGroupBy({self.obj}, keys={self.keys})"
