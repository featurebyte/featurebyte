"""
Utility functions for persistent
"""
from __future__ import annotations

import os

from featurebyte.persistent.base import Persistent
from featurebyte.persistent.mongo import MongoDB

DATABASE_NAME = os.environ.get("MONGODB_DB", "featurebyte")
MONGO_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27021")


def get_persistent() -> Persistent:
    """
    Return global Persistent object

    Returns
    -------
    Persistent
        Persistent object
    """
    return MongoDB(uri=MONGO_URI, database=DATABASE_NAME)
