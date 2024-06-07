"""
This module contain columns related metadata used in node definition.
"""

from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


class ColumnStr(str):
    """
    ColumnStr validator class
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        _ = source_type
        return core_schema.no_info_after_validator_function(cls, handler(str))


class OutColumnStr(ColumnStr):
    """
    OutColumnStr is used to type newly generated column string
    """


class InColumnStr(ColumnStr):
    """
    InColumnStr is used to type required input column string
    """
