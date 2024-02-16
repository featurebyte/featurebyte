"""
Utilities related to progress update
"""
from __future__ import annotations

from typing import Any, Callable, Coroutine


def get_ranged_progress_callback(
    progress_callback: Callable[..., Coroutine[Any, Any, None]],
    from_percent: int | float,
    to_percent: int | float,
) -> Callable[..., Coroutine[Any, Any, None]]:
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
    Callable[[int, ...], Coroutine[Any, Any, None]]
        New progress callback
    """
    assert from_percent < to_percent

    async def wrapped(percent: int, message: str | None, **kwargs: Any) -> None:
        effective_percent = percent / 100 * (to_percent - from_percent)
        await progress_callback(int(effective_percent + from_percent), message, **kwargs)

    return wrapped
