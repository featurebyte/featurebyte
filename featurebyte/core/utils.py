"""
Utility functions for API Objects
"""
from typing import Any

import asyncio
import threading


class RunThread(threading.Thread):
    """
    Thread to run async function in a different thread
    """

    def __init__(self, func: Any, args: Any, kwargs: Any) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.result = None
        super().__init__()

    def run(self) -> None:
        """
        Run async function
        """
        self.result = asyncio.run(self.func(*self.args, **self.kwargs))


def run_async(func: Any, *args: Any, **kwargs: Any) -> Any:
    """
    Run async function in both async and non-async context

    Parameters
    ----------
    func: Any
        Function to run
    args: Any
        Positional arguments
    kwargs: Any
        Keyword arguments

    Returns
    -------
    Any
        result from function call
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        thread = RunThread(func, args, kwargs)
        thread.start()
        thread.join()
        return thread.result
    return asyncio.run(func(*args, **kwargs))
