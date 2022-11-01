"""
Utility functions for persistent
"""
from __future__ import annotations

import os

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
        PERSISTENT = MongoDB(uri=os.environ.get("MONGODB_URI", "mongodb://localhost:27021"))
    return PERSISTENT
