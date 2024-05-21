"""
This module contains utility functions related to execution environment
"""

from __future__ import annotations

from typing import Any


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
        # pylint: disable=import-outside-toplevel
        from IPython.display import HTML, display

        display(HTML(html_content), metadata={"isolated": True})
