"""
Storage class
"""
from .mongo import MongoDB
from .persistent import DuplicateDocumentError, Persistent

__all__ = ["Persistent", "DuplicateDocumentError", "MongoDB"]
