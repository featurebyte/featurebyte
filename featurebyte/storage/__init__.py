"""
Storage classes
"""
from featurebyte.storage.base import Storage
from featurebyte.storage.local import LocalStorage

__all__ = ["Storage", "LocalStorage"]
