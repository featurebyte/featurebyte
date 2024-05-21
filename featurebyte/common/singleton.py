"""
Singleton metaclass
"""

from __future__ import annotations

from typing import Any


class SingletonMeta(type):
    """
    Singleton Metaclass for Singleton construction
    """

    _instances: dict[Any, Any] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]
