"""
__init__ file to load all modules within current directory
"""
from __future__ import annotations

from typing import Any

import importlib
import pkgutil
import sys


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


__all__ = list(import_submodules(__name__).keys())
