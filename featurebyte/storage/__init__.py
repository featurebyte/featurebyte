"""
Storage classes
"""
from featurebyte.storage.base import Storage
from featurebyte.storage.local import LocalStorage
from featurebyte.storage.local_temp import LocalTempStorage

__all__ = ["Storage", "LocalStorage", "LocalTempStorage"]
