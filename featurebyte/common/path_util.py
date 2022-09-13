"""
Utilities to retrieve paths related information
"""
import os


def get_package_root() -> str:
    """Get the path to the root of the package

    Returns
    -------
    str
    """
    root_dir = os.path.dirname(os.path.dirname(__file__))
    return root_dir
