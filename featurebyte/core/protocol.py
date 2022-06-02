"""
This module contains all protocol related classes
"""
from __future__ import annotations

from typing import Protocol


class HasRowIndexLineageProtocol(Protocol):
    """
    Class with row_index_lineage property/attribute
    """

    @property
    def row_index_lineage(self) -> tuple[str, ...]:
        """
        Attributes/property to indicate row index lineage

        Returns
        -------
        tuple[str, ...]
        """
