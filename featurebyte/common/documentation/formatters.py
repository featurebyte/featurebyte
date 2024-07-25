"""
Formatters
"""

import inspect
from enum import Enum
from typing import Any, ForwardRef, Optional, TypeVar

from pydantic_core import PydanticUndefined

from featurebyte.common.documentation.constants import EMPTY_VALUE
from featurebyte.common.documentation.util import _filter_none_from_list


def format_literal(value: Any) -> Optional[str]:
    """
    Format a literal value for display in documentation

    Parameters
    ----------
    value: Any
        Value to format

    Returns
    -------
    Optional[str]
        Formatted value
    """
    if value == EMPTY_VALUE:
        return None
    if isinstance(value, (str, Enum)):
        return f'"{value}"'
    return str(value)


def format_param_type(param_type: Any) -> Optional[str]:
    """
    Format parameter type and add reference if available

    Parameters
    ----------
    param_type: Any
        Parameter type

    Returns
    -------
    Optional[str]
        Formatted parameter type
    """

    def _get_param_type_str(param_type: Any) -> Optional[str]:
        if param_type == PydanticUndefined:
            return None
        if isinstance(param_type, TypeVar):
            # TypeVar spoofs the module
            return param_type.__name__
        if isinstance(param_type, ForwardRef):
            return param_type.__forward_arg__
        if getattr(param_type, "__module__", None) == "typing":
            return str(param_type)
        if not inspect.isclass(param_type):
            # non-class object
            return format_literal(param_type)

        # regular class
        module_str = getattr(param_type, "__module__", None)
        return f"{module_str}.{param_type.__name__}"

    def _clean(type_class_path: str) -> str:
        parts = type_class_path.split(",")
        if len(parts) > 1:
            formatted = [_format(part.strip()) for part in parts]
            filtered_list = _filter_none_from_list(formatted)
            return ", ".join(filtered_list)
        parts = type_class_path.split(".")
        if parts[-1]:
            object_name = parts[-1]
            if "#" in object_name:
                object_name = object_name.split("#")[-1]
            return object_name
        return type_class_path

    def _format(type_str: Optional[str]) -> Optional[str]:
        if not type_str:
            return type_str
        parts = type_str.split("[")
        if len(parts) == 1:
            return _clean(type_str)
        outer_left = _clean(parts.pop(0))
        type_str = "[".join(parts)
        parts = type_str.split("]")
        outer_right = parts.pop()
        type_str = "]".join(parts)
        return f"{outer_left}[{_format(type_str)}]{outer_right}"

    # check if type as subtypes
    module = getattr(param_type, "__module__", None)
    subtypes = getattr(param_type, "__args__", None)
    if module == "typing" and subtypes:
        if str(param_type).startswith("typing.Optional"):
            # __args__ attribute of Optional always includes NoneType as last element
            # which is redundant for documentation
            subtypes = subtypes[:-1]
        formatted_params = [format_param_type(subtype) for subtype in subtypes]
        filtered_formatted_params = _filter_none_from_list(formatted_params)
        inner_str = ", ".join(filtered_formatted_params)
        param_type_str = str(param_type)
        return _clean(param_type_str.split("[", maxsplit=1)[0]) + "[" + inner_str + "]"
    return _format(_get_param_type_str(param_type))
