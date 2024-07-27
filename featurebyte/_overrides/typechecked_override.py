"""
This module overrides the `typechecked` decorator from the `typeguard` package to customize
type checking behavior based on the module where the function is defined.
"""

from __future__ import annotations

import functools
import inspect
from typing import Any

import typeguard
from typeguard import CollectionCheckStrategy, ForwardRefPolicy, TypeCheckFailCallback, Unset
from typeguard import typechecked as original_typechecked
from typeguard._decorators import T_CallableOrType
from typeguard._utils import unset


def custom_typechecked(
    target: T_CallableOrType | None = None,
    *,
    forward_ref_policy: ForwardRefPolicy | Unset = unset,
    typecheck_fail_callback: TypeCheckFailCallback | Unset = unset,
    collection_check_strategy: CollectionCheckStrategy | Unset = unset,
    debug_instrumentation: bool | Unset = unset,
) -> Any:
    """
    Customize the `typechecked` decorator to selectively enable or disable type checking
    based on the defining module of the function or class.

    Parameters
    ----------
    target : T_CallableOrType | None
        the function or class to enable type checking for
    forward_ref_policy: ForwardRefPolicy | Unset
        override for
    typecheck_fail_callback: TypeCheckFailCallback | Unset
        override for
    collection_check_strategy: CollectionCheckStrategy | Unset
        override for
    debug_instrumentation: bool | Unset
        override for

    Returns
    -------
    Any
        Either the decorated function or a partial object, depending on whether `func` is provided.
    """
    if target is None:
        return functools.partial(
            custom_typechecked,
            forward_ref_policy=forward_ref_policy,
            typecheck_fail_callback=typecheck_fail_callback,
            collection_check_strategy=collection_check_strategy,
            debug_instrumentation=debug_instrumentation,
        )

    module = inspect.getmodule(target)
    if module and module.__name__.startswith("feast.feature_view"):
        return target

    return original_typechecked(  # type: ignore
        target=target,
        forward_ref_policy=forward_ref_policy,
        typecheck_fail_callback=typecheck_fail_callback,
        collection_check_strategy=collection_check_strategy,
        debug_instrumentation=debug_instrumentation,
    )


# Override the typechecked function in the typeguard module
typeguard.typechecked = custom_typechecked
