"""
This module contains descriptor class.
"""
from typing import Any, Callable, Optional, Type, TypeVar

from functools import partial

MethodT = Callable[..., Any]
T = TypeVar("T")


class ClassInstanceMethodDescriptor:
    """
    ListFeatureVersionDescriptor class
    """

    def __init__(self, class_method: MethodT, instance_method: MethodT):
        self._class_method = class_method
        self._instance_method = instance_method

    def __get__(self, instance: Optional[T], owner: Type[T]) -> MethodT:
        if instance is None:
            # @classmethod decorated method should have __func__ attribute
            return partial(self._class_method.__func__, cls=owner)  # type: ignore
        return partial(self._instance_method, self=instance)
