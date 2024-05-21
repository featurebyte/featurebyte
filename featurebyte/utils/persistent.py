"""
Utility functions for persistent
"""

from __future__ import annotations

import os

from featurebyte.persistent.mongo import MongoDB

DATABASE_NAME = os.environ.get("MONGODB_DB", "featurebyte")
MONGO_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")


class MongoDBImpl(MongoDB):
    """
    Default MongoDB implementation for featurebyte
    """

    def __init__(self) -> None:
        """
        Initialize MongoDBImpl
        """
        super().__init__(uri=MONGO_URI, database=DATABASE_NAME)
