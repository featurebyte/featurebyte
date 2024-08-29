"""
SourceInfo class
"""

from __future__ import annotations

from dataclasses import dataclass

from featurebyte.enum import SourceType


@dataclass
class SourceInfo:
    """
    Information about the source of the feature store needed for SQL generation
    """

    database_name: str
    schema_name: str
    source_type: SourceType
