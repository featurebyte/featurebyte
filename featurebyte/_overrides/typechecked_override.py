"""
This module overrides the `typechecked` decorator from the `typeguard` package to customize
type checking behavior based on the module where the function is defined.
"""

from typing import Any

import functools
import inspect

import typeguard
from typeguard import typechecked as original_typechecked


def custom_typechecked(
    func: Any = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Customize the `typechecked` decorator to selectively enable or disable type checking
    based on the defining module of the function or class.

    Parameters
    ----------
    func : Any
        The function or class to enable type checking for.
    *args : Any
        Additional positional arguments to pass to the original `typechecked` decorator.
    **kwargs : Any
        Additional keyword arguments to pass to the original `typechecked` decorator.

    Returns
    -------
    Any
        Either the decorated function or a partial object, depending on whether `func` is provided.
    """
    if func is None:
        return functools.partial(custom_typechecked, *args, **kwargs)

    module = inspect.getmodule(func)
    if module and module.__name__.startswith("feast.feature_view"):
        return func

    return original_typechecked(func, *args, **kwargs)  # type: ignore


# Override the typechecked function in the typeguard module
typeguard.typechecked = custom_typechecked
