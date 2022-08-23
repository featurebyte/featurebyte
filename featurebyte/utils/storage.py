"""
Utility functions for file storage
"""
from __future__ import annotations

from typing import Optional

from featurebyte.config import Configurations
from featurebyte.storage import LocalStorage, Storage

STORAGE: Optional[Storage] = None


def get_storage() -> Storage:
    """
    Return global Storage object

    Returns
    -------
    Storage
        Storage object
    """
    global STORAGE  # pylint: disable=global-statement
    if not STORAGE:
        config = Configurations()
        STORAGE = LocalStorage(base_path=config.storage.local_path)
    return STORAGE
