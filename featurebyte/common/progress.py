"""
Utilities related to progress update
"""

from __future__ import annotations

from typing import Any, Callable, Coroutine, Generator, Tuple, TypeVar

import numpy as np

ProgressCallbackType = Callable[..., Coroutine[Any, Any, None]]
T = TypeVar("T")


def get_ranged_progress_callback(
    progress_callback: Callable[..., Coroutine[Any, Any, None]],
    from_percent: int | float,
    to_percent: int | float,
    clip_start_threshold: int | None = None,
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
    clip_start_threshold: int | None
        Clip the start value of the progress callback if it is below this threshold.
        This is used to avoid noisy estimated total runtime time at the beginning of the progress.
        For example, it takes 10 minutes at 1%, 11 minutes at 3%, 12 minutes at 5%, 15 minutes at 10%.
        If we estimate the total runtime at 1%, it will be 1500 minutes, at 3% it will be 550 minutes,
        at 5% it will be 240 minutes, at 10% it will be 150 minutes. If we clip the start value at 5%,
        the estimated total runtime will be 240 minutes at 5%, 150 minutes at 10%. There is no estimation
        at 1% and 3% because the progress is clipped to 5% and the estimated total runtime is not updated.
        This gives a more stable and accurate estimate of the total runtime.

    Returns
    -------
    ProgressCallbackType
        New progress callback
    """
    assert from_percent < to_percent

    async def wrapped(percent: int, message: str | None, **kwargs: Any) -> None:
        effective_percent = percent / 100 * (to_percent - from_percent)
        update_value = int(effective_percent + from_percent)
        if clip_start_threshold is not None and update_value < clip_start_threshold:
            update_value = int(from_percent)
        await progress_callback(update_value, message, **kwargs)

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


def ranged_progress_callback_iterator(
    items: list[T], progress_callback: ProgressCallbackType
) -> Generator[Tuple[T, ProgressCallbackType], None, None]:
    """
    Returns a generator that yields items and a progress callback that updates the progress based on the number

    Parameters
    ----------
    items: list[T]
        List of items to iterate over
    progress_callback: ProgressCallbackType
        Progress callback to update the progress

    Yields
    ------
    Tuple[T, ProgressCallbackType]
        Item and progress callback
    """
    total_items = len(items)
    for i, item in enumerate(items):
        from_percent = int(np.floor((i / total_items) * 100))
        to_percent = int(np.ceil(((i + 1) / total_items) * 100))
        ranged_callback = get_ranged_progress_callback(
            progress_callback,
            from_percent,
            to_percent,
        )
        yield item, ranged_callback
