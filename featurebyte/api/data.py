"""
Data class
"""
from __future__ import annotations

from typing import Any

from featurebyte.api.base_data import DataApiObject
from featurebyte.api.dimension_data import DimensionData
from featurebyte.api.event_data import EventData
from featurebyte.api.item_data import ItemData
from featurebyte.api.scd_data import SlowlyChangingData
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.models.tabular_data import TabularDataModel


class Data(TabularDataModel, DataApiObject):
    """
    Data class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["Data"],
        proxy_class="featurebyte.Data",
    )

    @classmethod
    def get(cls: Any, name: str) -> Any:
        """
        Retrieve lazy object from the persistent given object name

        Parameters
        ----------
        name: str
            Object name

        Returns
        -------
        Any
            Retrieved object with the specified name
        """
        data = cls._get(name)
        data_class = {
            TableDataType.EVENT_DATA: EventData,
            TableDataType.ITEM_DATA: ItemData,
            TableDataType.SCD_DATA: SlowlyChangingData,
            TableDataType.DIMENSION_DATA: DimensionData,
        }[data.type]
        return data_class.get(name)
