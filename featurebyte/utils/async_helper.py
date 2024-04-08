"""
Helper functions for asyncio
"""

from __future__ import annotations

from typing import Any, Coroutine, List

import asyncio
from asyncio import Future


def asyncio_gather(*coros: Coroutine[Any, Any, Any], max_concurrency: int = 0) -> Future[List[Any]]:
    """
    Run coroutines with a optional concurrency limit

    Parameters
    ----------
    coros: Coroutine[Any, Any, Any]
        Coroutines to run
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
    semaphore = asyncio.Semaphore(max_concurrency)

    # Run coroutines with a semaphore
    async def _coro(coro: Coroutine[Any, Any, Any]) -> Any:
        async with semaphore:
            nonlocal failed, tasks, tasks_canceled
            if failed:  # pylint: disable=used-before-assignment
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
