"""
Resource util.
"""

import inspect
from typing import Any

from mkautodoc.extension import import_from_string


def import_resource(resource_descriptor: str) -> Any:
    """
    Import module

    Parameters
    ----------
    resource_descriptor: str
        Resource descriptor path

    Returns
    -------
    Any
    """
    resource = import_from_string(resource_descriptor)
    module = inspect.getmodule(resource)
    if module is None:
        return resource
    return getattr(module, resource.__name__)
