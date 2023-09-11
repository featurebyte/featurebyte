"""
This module contains a decorator to retry a coroutine function
"""
from typing import Any, Callable, Optional, Tuple, Type, Union

import asyncio
import random
from functools import wraps


def async_retry(
    max_retries: int = 3,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    backoff_factor: float = 2,
    exception_signature_check: Optional[Callable[[BaseException], bool]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Retry a coroutine function

    Parameters
    ----------
    max_retries: int
        Maximum number of retries
    exceptions: Tuple[Type[BaseException], ...]
        Exceptions to be caught
    backoff_factor: float
        Backoff factor
    exception_signature_check:
        A function to check if the exception signature is valid for retry

    Returns
    -------
    Callable[[Callable[..., Any]], Callable[..., Any]]

    # noqa: DAR005
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def retry_wrapper(
            *args: Tuple[Union[int, str], ...], **kwargs: Union[int, str]
        ) -> Any:
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    if exception_signature_check and not exception_signature_check(exc):
                        raise  # If the exception doesn't pass the check, re-raise it
                    retries += 1
                    delay = backoff_factor**retries
                    await asyncio.sleep(delay + random.uniform(0, 1))  # Add jitter
            return await func(*args, **kwargs)  # Retry exhausted, raise the last exception

        return retry_wrapper

    return decorator
