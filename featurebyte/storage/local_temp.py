"""
Local temp storage class
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from featurebyte.storage.local import LocalStorage


class LocalTempStorage(LocalStorage):
    """
    Local temp storage class
    """

    def __init__(self) -> None:
        """
        Initialize local temp storage location
        """
        super().__init__(base_path=Path(tempfile.gettempdir()))
