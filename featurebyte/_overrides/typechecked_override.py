"""
This module overrides the `typechecked` decorator from the `typeguard` package to customize
type checking behavior based on the module where the function is defined.
"""

from typing import Any, Dict, Optional

import functools
import inspect

import typeguard
from typeguard import typechecked as original_typechecked


def custom_typechecked(
    func: Any = None,
    *,
    always: bool = False,
    _localns: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Customize the `typechecked` decorator to selectively enable or disable type checking
    based on the defining module of the function or class.

    Parameters
    ----------
    func : Any
        The function or class to enable type checking for.
    always : bool
        True to enable type checks even in optimized mode.
    _localns : Optional[Dict[str, Any]]
        Local namespace used for resolving type annotations.

    Returns
    -------
    Any
        Either the decorated function or a partial object, depending on whether `func` is provided.
    """
    if func is None:
        return functools.partial(custom_typechecked, always=always, _localns=_localns)

    module = inspect.getmodule(func)
    if module and module.__name__.startswith("feast.feature_view"):
        return func

    return original_typechecked(func, always=always, _localns=_localns)  # type: ignore


# Override the typechecked function in the typeguard module
typeguard.typechecked = custom_typechecked
