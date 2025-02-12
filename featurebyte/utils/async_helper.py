"""
Helper functions for asyncio
"""

from __future__ import annotations

import asyncio
import time
import uuid
from asyncio import Future, Task
from typing import Any, Coroutine, List

from redis import Redis
from redis.exceptions import LockNotOwnedError

from featurebyte.session.base import LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS

SEMAPHORE_TIMEOUT_SECONDS = 30


class RedisFairCountingSemaphore:
    """
    Fair counting semaphore using Redis to manage concurrency limits across multiple clients.
    Adapted from: "Redis In Action" by Josiah L. Carlson (Manning Publications 2013) Chapter 6.3 (pages 127 - 134)
    """

    def __init__(self, redis: Redis[Any], key: str, limit: int, timeout: int = 10):
        """
        Initialize semaphore

        Parameters
        ----------
        redis: Redis[Any]
            Redis connection
        key: str
            Semaphore key
        limit: int
            Semaphore limit count
        timeout: int
            Timeout to acquire semaphore in seconds
        """
        self._redis = redis
        self._key = key
        self._limit = limit
        self._timeout = timeout
        self._czset = f"{self._key}:owner"
        self._ctr = f"{self._key}:counter"
        self._lock = f"{self._key}:lock"
        self._identifier: str | None = None
        self._semaphore_refresh_task: Task[None] | None = None

    async def _acquire_semaphore(self) -> str | None:
        """
        Try to acquire semaphore using redis.
        At any given time, the number of semaphores acquired must not exceed the limit.

        The implementation involves a Redis counter, and two sorted sets (Redis zsets).
        - The counter provides a fair and unique position in the request queue
        - The first sorted set (identity zset) tracks the creation time of each semaphore for expiry management
        - The second sorted set (owner zset) provides a list of semaphores sorted by request order.

        When a semaphore is requested, the following steps are taken:

        - Remove expired semaphore records from the identity and owner zsets based on creation time and timeout
        - Get queue position from the counter. The counter is atomic and returns an incrementing integer value
        - Create a unique semaphore identifier and add it to the identity zset with the current time as the score
        - Add identifier to the owner zset with queue position as the score
        - Check if semaphore is acquired based on it's rank in the owner zset vs the limit
        - If acquired, return the semaphore identifier
        - If not acquired, remove the semaphore identifier from the identity and owner zsets

        Returns
        -------
        str | None
            Semaphore identifier if acquired, None otherwise
        """
        # Create a pipeline to execute multiple commands atomically
        pipeline = self._redis.pipeline(True)

        # Remove expired semaphore records from the identity and owner zsets
        now = time.time()
        pipeline.zremrangebyscore(self._key, "-inf", now - SEMAPHORE_TIMEOUT_SECONDS)
        pipeline.zinterstore(self._czset, {self._czset: 1, self._key: 0})

        # Get queue position from redis counter
        pipeline.incr(self._ctr)
        counter = pipeline.execute()[-1]

        # create a unique identifier for the semaphore and add it to the identity zset
        identifier = str(uuid.uuid4())
        pipeline.zadd(self._key, {identifier: now})

        # add identifier to the owner zset with queue position as the score
        pipeline.zadd(self._czset, {identifier: counter})

        # check if semaphore is acquired based on it's rank in the owner zset vs the limit
        pipeline.zrank(self._czset, identifier)
        rank = pipeline.execute()[-1]
        if rank < self._limit:
            # Semaphore acquired - return identifier
            return identifier

        # Semaphore not acquired - clear identifier from identity and owner zsets
        pipeline.zrem(self._key, identifier)
        pipeline.zrem(self._czset, identifier)
        pipeline.execute()
        return None

    async def _release_semaphore(self) -> None:
        """
        Release semaphore
        """
        if not self._identifier:
            return

        # Cancel semaphore refresh task
        if self._semaphore_refresh_task:
            self._semaphore_refresh_task.cancel()

        # Release semaphore
        pipeline = self._redis.pipeline(True)
        pipeline.zrem(self._key, self._identifier)
        pipeline.zrem(self._czset, self._identifier)
        pipeline.execute()
        self._identifier = None

    async def _refresh(self) -> bool:
        """
        Refresh semaphore

        Returns
        -------
        bool
            True if semaphore is refreshed, False
        """
        if not self._identifier:
            return False

        # Refresh semaphore
        if self._redis.zadd(self._key, {self._identifier: time.time()}):
            # semaphore is lost - clean up
            await self._release_semaphore()
            return False

        # Semaphore refreshed
        return True

    async def __aenter__(self) -> RedisFairCountingSemaphore:
        """
        Acquire semaphore on enter, blocks until semaphore is acquired or timeout

        Raises
        ------
        TimeoutError
            If semaphore is not acquired within timeout
        RuntimeError
            If semaphore is already acquired

        Returns
        -------
        RedisFairCountingSemaphore
        """
        if self._identifier:
            raise RuntimeError("Semaphore already acquired")

        async def _semaphore_refresh_task() -> None:
            """
            Refresh semaphore periodically
            """
            while self._identifier is not None:
                # Refresh semaphore while not released
                await asyncio.sleep(SEMAPHORE_TIMEOUT_SECONDS / 2)
                await self._refresh()

        # Try to acquire semaphore
        now = time.time()
        while time.time() - now < self._timeout:
            # Acquire lock for semaphore
            lock = self._redis.lock(self._lock, timeout=10)
            if lock.acquire(blocking=False):
                try:
                    identifier = await self._acquire_semaphore()
                    if identifier:
                        self._identifier = identifier
                        # Add task to refresh semaphore while semaphore is acquired
                        self._semaphore_refresh_task = asyncio.create_task(
                            _semaphore_refresh_task()
                        )
                        return self
                finally:
                    # Release lock
                    if lock.owned():
                        try:
                            lock.release()
                        except LockNotOwnedError:
                            # Lock may have expired before release
                            pass
            await asyncio.sleep(0.1)
        # Timeout trying to acquire semaphore
        raise TimeoutError(f"Failed to acquire semaphore after {time.time() - now}s")

    async def __aexit__(self, *exc: dict[str, Any]) -> None:
        """
        Release semaphore on exit

        Parameters
        ----------
        exc: dict[str, Any]
            parameters
        """
        await self._release_semaphore()


def asyncio_gather(
    *coros: Coroutine[Any, Any, Any],
    redis: Redis[Any],
    concurrency_key: str,
    max_concurrency: int = 0,
    return_exceptions: bool = False,
) -> Future[List[Any]]:
    """
    Run coroutines with a optional concurrency limit

    Parameters
    ----------
    coros: Coroutine[Any, Any, Any]
        Coroutines to run
    redis: Redis[Any]
        Redis connection
    concurrency_key: str
        Key for concurrency limit enforcement
    max_concurrency: int
        Maximum number of coroutines to run concurrently
    return_exceptions: bool
        When True, exceptions are gathered in the result list instead of being raised immediately
        and will not trigger cancellation of other tasks.

    Returns
    -------
    Future[List[Any]]
    """

    if max_concurrency < 1:
        return asyncio.gather(*coros, return_exceptions=return_exceptions)

    failed = False
    tasks_canceled = False
    tasks = []
    semaphore_key = f"asyncio_gather:{concurrency_key}"

    # Run coroutines with a semaphore
    async def _coro(coro: Coroutine[Any, Any, Any]) -> Any:
        async with RedisFairCountingSemaphore(
            redis=redis,
            key=semaphore_key,
            limit=max_concurrency,
            timeout=LONG_RUNNING_EXECUTE_QUERY_TIMEOUT_SECONDS,
        ):
            nonlocal failed, tasks, tasks_canceled
            if failed:
                # Close unawaited coroutines on failure
                coro.close()
                return
            task = asyncio.create_task(coro)
            tasks.append(task)
            try:
                return await task
            except Exception:
                if return_exceptions:
                    # Do not cancel other tasks if return_exceptions is True since failures are
                    # expected to be handled by the caller
                    raise
                failed = True
                if not tasks_canceled:
                    # Cancel all tasks on failure
                    for task in tasks:
                        task.cancel()
                    tasks_canceled = True
                raise

    return asyncio.gather(*(_coro(c) for c in coros), return_exceptions=return_exceptions)
