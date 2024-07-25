"""
Utilities to retrieve paths related information
"""

from __future__ import annotations

import importlib
import os
import pkgutil
import sys
from typing import Any


def get_package_root() -> str:
    """Get the path to the root of the package

    Returns
    -------
    str
    """
    root_dir = os.path.dirname(os.path.dirname(__file__))
    return root_dir


def import_submodules(package_name: str) -> dict[str, Any]:
    """
    Import all submodules of a module, recursively

    Parameters
    ----------
    package_name: str
        Package name

    Returns
    -------
    dict[str, Any]
    """
    package = sys.modules[package_name]
    return {
        name: importlib.import_module(package_name + "." + name)
        for loader, name, is_pkg in pkgutil.walk_packages(package.__path__)
    }
