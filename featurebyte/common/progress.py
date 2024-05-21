"""
Utilities related to progress update
"""

from __future__ import annotations

from typing import Any, Callable, Coroutine, Tuple

ProgressCallbackType = Callable[..., Coroutine[Any, Any, None]]


def get_ranged_progress_callback(
    progress_callback: Callable[..., Coroutine[Any, Any, None]],
    from_percent: int | float,
    to_percent: int | float,
) -> ProgressCallbackType:
    """
    Returns a new progress callback that maps the progress range from [0, 100] to [from_percent,
    to_percent] of the original progress callback.

    Parameters
    ----------
    progress_callback: Callable[[int, ...], Coroutine[Any, Any, None]]
        Original progress callback
    from_percent: int | float
        Lower bound of the new progress range
    to_percent: int | float
        Upper bound of the new progress range

    Returns
    -------
    ProgressCallbackType
        New progress callback
    """
    assert from_percent < to_percent

    async def wrapped(percent: int, message: str | None, **kwargs: Any) -> None:
        effective_percent = percent / 100 * (to_percent - from_percent)
        await progress_callback(int(effective_percent + from_percent), message, **kwargs)

    return wrapped


def divide_progress_callback(
    progress_callback: Callable[..., Coroutine[Any, Any, None]],
    at_percent: int | float,
) -> Tuple[ProgressCallbackType, ProgressCallbackType]:
    """
    Divide a progress callback into two ranged progress callbacks at the specified percent

    Parameters
    ----------
    progress_callback: Callable[[int, ...], Coroutine[Any, Any, None]]
        Original progress callback
    at_percent: int | float
        Percentage to divide at

    Returns
    -------
    Tuple[ProgressCallbackType, ProgressCallbackType]
    """
    return get_ranged_progress_callback(
        progress_callback,
        0,
        at_percent,
    ), get_ranged_progress_callback(
        progress_callback,
        at_percent,
        100,
    )
