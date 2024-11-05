"""
Redis utilities
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from redis import Redis
from redis.exceptions import LockNotOwnedError
from redis.lock import Lock


@asynccontextmanager
async def acquire_lock(
    redis: Redis[Any],
    name: str,
    timeout: Optional[int] = None,
    blocking_timeout: Optional[int] = None,
) -> AsyncIterator[Lock]:
    """
    Asynchronously acquires a Redis lock, handling scenarios where the lock is not owned on release.

    This context manager attempts to acquire a lock with the specified `name` in Redis.
    If a `timeout` is provided, the lock will automatically release after the specified duration.
    The `blocking_timeout`, if given, sets the maximum time the method will attempt to acquire the
    lock before raising a `TimeoutError`.

    Parameters
    ----------
    redis: Redis[Any]
        Redis client instance.
    name: str
        Name of the lock.
    timeout: Optional[int]
        Maximum duration in seconds for which the lock should be held. If not specified, the lock
        will be held indefinitely until explicitly released.
    blocking_timeout: Optional[int]
        Maximum duration in seconds to wait for acquiring the lock. If not specified, the method
        will attempt to acquire the lock indefinitely.

    Yields
    ------
    Lock
        The acquired lock instance.

    Raises
    ------
    TimeoutError
        If the lock cannot be acquired within the specified `blocking_timeout`.
    """
    now = time.time()

    lock = redis.lock(name, timeout=timeout)
    lock_acquired = False

    while blocking_timeout is None or time.time() - now < blocking_timeout:
        if lock.acquire(blocking=False):
            lock_acquired = True
            break
        await asyncio.sleep(0.1)

    if not lock_acquired:
        raise TimeoutError(f"Failed to acquire lock after {time.time() - now}s")

    try:
        yield lock
    finally:
        if lock.owned():
            try:
                lock.release()
            except LockNotOwnedError:
                # Lock may have expired before release
                pass
