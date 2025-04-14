"""
This module contains utility functions related to execution environment
"""

from __future__ import annotations

import os
from contextlib import ExitStack, contextmanager
from typing import Any, Dict, Iterator, List


def is_notebook() -> bool:
    """
    Check whether it is in the jupyter notebook

    Reference: https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook

    Returns
    -------
    bool
    """
    try:
        shell = get_ipython().__class__.__name__  # type: ignore
        return bool(shell in ["ZMQInteractiveShell", "Shell", "DatabricksShell"])
    except NameError:
        return False


def get_alive_bar_additional_params() -> dict[str, Any]:
    """
    Get alive_bar additional parameters based on running environment

    Returns
    -------
    dict[str, Any]
    """
    if is_notebook():
        return {"force_tty": True}
    return {"dual_line": True}


def display_html_in_notebook(html_content: str) -> None:
    """
    Display html content in notebook environment

    Parameters
    ----------
    html_content: str
        HTML content to display
    """

    if is_notebook():
        from IPython.display import HTML, display

        display(HTML(html_content), metadata={"isolated": True})


@contextmanager
def set_environment_variable(variable: str, value: Any) -> Iterator[None]:
    """
    Set the environment variable within the context

    Parameters
    ----------
    variable: str
        The environment variable
    value: Any
        The value to set

    Yields
    ------
    Iterator[None]
        The context manager
    """
    previous_value = os.environ.get(variable)
    os.environ[variable] = value

    try:
        yield
    finally:
        if previous_value is not None:
            os.environ[variable] = previous_value
        else:
            del os.environ[variable]


@contextmanager
def set_environment_variables(variables: Dict[str, Any]) -> Iterator[None]:
    """
    Set multiple environment variables within the context

    Parameters
    ----------
    variables: Dict[str, Any]
        Key value mapping of environment variables to set

    Yields
    ------
    Iterator[None]
        The context manager
    """
    ctx_managers: List[Any] = []

    for key, value in variables.items():
        ctx_managers.append(set_environment_variable(key, value))

    if ctx_managers:
        with ExitStack() as stack:
            for mgr in ctx_managers:
                stack.enter_context(mgr)
            yield
    else:
        yield


def is_development_mode() -> bool:
    """
    Check whether it is in development mode

    Returns
    -------
    bool
    """
    return os.environ.get("FEATUREBYTE_EXECUTION_MODE") == "development"


def is_io_worker() -> bool:
    """
    Check whether the code is running in IO worker

    Returns
    -------
    bool
    """
    return "worker-io" in os.environ.get("HOSTNAME", "")


def is_feature_query_debug_enabled() -> bool:
    """
    Check whether the feature query debugging is enabled

    Returns
    -------
    bool
        True if debugging is enabled, False otherwise
    """
    return os.environ.get("FEATUREBYTE_FEATURE_QUERY_DEBUG") == "1"
