"""
Storage class
"""
from .mongo import MongoStorage
from .storage import Storage

__all__ = ["Storage", "MongoStorage"]
