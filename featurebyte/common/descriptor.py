"""
This module contains descriptor class.
"""

from functools import partial
from typing import Any, Callable, Optional, Type, TypeVar

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
            class_method = self._class_method.__func__  # type: ignore
            method = partial(class_method, cls=owner)
            method.__doc__ = class_method.__doc__
        else:
            method = partial(self._instance_method, self=instance)
            method.__doc__ = self._instance_method.__doc__
        return method
