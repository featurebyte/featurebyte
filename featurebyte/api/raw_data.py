"""
This module contains raw accessor class
"""
from __future__ import annotations

from pydantic import Field

from featurebyte.api.database_table import AbstractTableDataFrame
from featurebyte.core.frame import BaseFrame
from featurebyte.models import FeatureStoreModel


class RawAccessorMixin:
    """
    RawAccessorMixin
    """

    @property
    def raw(self: AbstractTableDataFrame):  # type: ignore # pylint: disable=invalid-name
        """
        raw accessor object

        Returns
        -------
        RawTableData
        """
        return RawTableData.create(obj=self)


class RawTableData(BaseFrame):
    """
    RawTableData class
    """

    feature_store: FeatureStoreModel = Field(allow_mutation=False, exclude=True)

    @classmethod
    def create(cls, obj: AbstractTableDataFrame):
        return RawTableData()
