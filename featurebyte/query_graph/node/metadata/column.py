"""
This module contain columns related metadata used in node definition.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pydantic.typing import CallableGenerator


class ColumnStr(str):
    """
    ColumnStr validator class
    """

    @classmethod
    def __get_validators__(cls) -> "CallableGenerator":
        yield cls.validate

    @classmethod
    def validate(cls, value: Any) -> "ColumnStr":
        """
        Validate value

        Parameters
        ----------
        value: Any
            Input to the ColumnStr class

        Returns
        -------
        ColumnStr
        """
        return cls(str(value))


class OutColumnStr(ColumnStr):
    """
    OutColumnStr is used to type newly generated column string
    """


class InColumnStr(ColumnStr):
    """
    InColumnStr is used to type required input column string
    """
