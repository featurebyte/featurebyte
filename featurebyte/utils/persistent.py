"""
Utility functions for persistent
"""
from __future__ import annotations

from featurebyte.config import Configurations
from featurebyte.persistent.base import Persistent
from featurebyte.persistent.mongo import MongoDB

PERSISTENT = None


def get_persistent() -> Persistent:
    """
    Return global Persistent object

    Returns
    -------
    Persistent
        Persistent object
    """
    global PERSISTENT  # pylint: disable=global-statement
    if not PERSISTENT:
        PERSISTENT = MongoDB("mongodb://localhost:27021")
    return PERSISTENT
