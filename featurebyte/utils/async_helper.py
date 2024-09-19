"""
Helper functions for asyncio
"""

from __future__ import annotations

import asyncio
import time
import uuid
from asyncio import Future
from typing import Any, Coroutine, List

from redis import Redis


class RedisFairCountingSemaphore:
    """
    Fair counting semaphore using Redis adapted from "Redis In Action" by Josiah L. Carlson
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
            Timeout in seconds
        """
        self._redis = redis
        self._key = key
        self._limit = limit
        self._timeout = timeout
        self._czset = f"{self._key}:owner"
        self._ctr = f"{self._key}:counter"
        self._lock = f"{self._key}:lock"
        self._identifier: str | None = None

    async def __aenter__(self) -> None:
        """
        Acquire semaphore on enter, blocks until semaphore is acquired or timeout

        Raises
        ------
        TimeoutError
            If semaphore is not acquired within timeout
        """
        # Acquire lock for semaphore
        lock_timeout = 1
        lock = self._redis.lock(self._lock, timeout=lock_timeout)
        now = time.time()
        while not lock.acquire(blocking=False):
            if time.time() - now > self._timeout:
                raise TimeoutError(f"Failed to acquire lock: {time.time() - now}")
            await asyncio.sleep(0.1)

        # Increment distributed counter
        try:
            counter = self._redis.incr(self._ctr)
        finally:
            # Release lock
            if lock.owned():
                lock.release()

        now = time.time()
        self._identifier = str(uuid.uuid4())
        pipeline = self._redis.pipeline(True)

        # Remove expired semaphores
        pipeline.zremrangebyscore(self._key, "-inf", now - self._timeout)
        pipeline.zinterstore(self._czset, {self._czset: 1, self._key: 0})

        # Acquire semaphore
        pipeline.zadd(self._key, {self._identifier: now})
        pipeline.zadd(self._czset, {self._identifier: counter})
        pipeline.zrank(self._czset, self._identifier)
        rank = pipeline.execute()[-1]
        if rank < self._limit:
            return

        while time.time() - now < self._timeout:
            rank = self._redis.zrank(self._czset, self._identifier)
            if rank is None:
                # Semaphore was released
                break
            if rank < self._limit:
                return
            await asyncio.sleep(0.1)

        # Semaphore not acquired within timeout
        pipeline.zrem(self._key, self._identifier)
        pipeline.zrem(self._czset, self._identifier)
        pipeline.execute()
        self._identifier = None
        raise TimeoutError("Failed to acquire semaphore")

    async def __aexit__(self, *exc: dict[str, Any]) -> None:
        """
        Release semaphore on exit

        Parameters
        ----------
        exc: dict[str, Any]
            parameters
        """
        if not self._identifier:
            return

        # Release semaphore
        pipeline = self._redis.pipeline(True)
        pipeline.zrem(self._key, self._identifier)
        pipeline.zrem(self._czset, self._identifier)
        pipeline.execute()


def asyncio_gather(
    *coros: Coroutine[Any, Any, Any], redis: Redis[Any], max_concurrency: int = 0
) -> Future[List[Any]]:
    """
    Run coroutines with a optional concurrency limit

    Parameters
    ----------
    coros: Coroutine[Any, Any, Any]
        Coroutines to run
    redis: Redis[Any]
        Redis connection
    max_concurrency: int
        Maximum number of coroutines to run concurrently

    Returns
    -------
    Future[List[Any]]
    """

    if max_concurrency < 1:
        return asyncio.gather(*coros)

    failed = False
    tasks_canceled = False
    tasks = []

    # Run coroutines with a semaphore
    async def _coro(coro: Coroutine[Any, Any, Any]) -> Any:
        async with RedisFairCountingSemaphore(
            redis=redis, key="asyncio_gather", limit=max_concurrency, timeout=3600
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
                failed = True
                if not tasks_canceled:
                    # Cancel all tasks on failure
                    for task in tasks:
                        task.cancel()
                    tasks_canceled = True
                raise

    return asyncio.gather(*(_coro(c) for c in coros))
