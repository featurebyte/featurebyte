"""
This module contains utility functions related to execution environment
"""


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
        return bool(shell == "ZMQInteractiveShell")
    except NameError:
        return False
