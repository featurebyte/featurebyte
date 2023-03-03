"""
Start worker
"""
from typing import Any

from asyncio import get_event_loop

from celery.signals import worker_process_shutdown

from . import celery


@worker_process_shutdown.connect
def process_shutdown(*args: Any, **kwargs: Any) -> None:
    """
    Shutdown event loop

    Parameters
    ----------
    args: Any
        Arguments
    kwargs: Any
        Keyword arguments
    """
    _ = args, kwargs
    loop = get_event_loop()
    loop.close()


celery.autodiscover_tasks(["featurebyte.worker.task_executor"])
