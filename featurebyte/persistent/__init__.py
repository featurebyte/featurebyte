"""
Storage class
"""
from .mongo import MongoDB
from .persistent import Persistent

__all__ = ["Persistent", "MongoDB"]
