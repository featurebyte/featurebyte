"""
Block modification handler
"""

from typing import Iterator

from contextlib import contextmanager


class BlockModificationHandler:
    """
    Block modification handler
    """

    def __init__(self) -> None:
        self._enable_block_modification_check = True

    @contextmanager
    def disable_block_modification_check(self) -> Iterator[None]:
        """
        Disable block modification check.

        Yields
        ------
        LazyAppContainer
        """
        try:
            self._enable_block_modification_check = False
            yield
        finally:
            self._enable_block_modification_check = True

    @property
    def block_modification(self) -> bool:
        """
        Check if block modification is enabled

        Returns
        -------
        bool
        """
        return self._enable_block_modification_check
