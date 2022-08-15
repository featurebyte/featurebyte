"""
This module contains datetime accessor class
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from featurebyte.enum import DBVarType

if TYPE_CHECKING:
    from featurebyte.core.series import Series


class DtAccessorMixin:
    """
    StrAccessorMixin class
    """

    # pylint: disable=too-few-public-methods

    @property
    def dt(self: Series) -> DatetimeAccessor:  # type: ignore # pylint: disable=invalid-name
        """
        dt accessor object

        Returns
        -------
        DatetimeAccessor
        """
        return DatetimeAccessor(self)


class DatetimeAccessor:
    """
    DatetimeAccessor class used to manipulate datetime-like type Series object
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, obj: Series):
        if obj.var_type != DBVarType.TIMESTAMP:
            raise AttributeError("Can only use .dt accessor with TIMESTAMP values!")
        self._obj = obj
