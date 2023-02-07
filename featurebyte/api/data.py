"""
Data class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.api.base_data import DataListMixin
from featurebyte.api.dimension_data import DimensionData
from featurebyte.api.event_data import EventData
from featurebyte.api.item_data import ItemData
from featurebyte.api.scd_data import SlowlyChangingData
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.models.tabular_data import TabularDataModel


class Data(TabularDataModel, DataListMixin):
    """
    Data class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["Data"],
        proxy_class="featurebyte.Data",
    )

    # class variables
    _get_schema = TabularDataModel

    _data_type_to_cls_mapping = {
        TableDataType.EVENT_DATA: EventData,
        TableDataType.ITEM_DATA: ItemData,
        TableDataType.SCD_DATA: SlowlyChangingData,
        TableDataType.DIMENSION_DATA: DimensionData,
    }

    @classmethod
    def get(cls: Any, name: str) -> Any:
        """
        Retrieve saved data source by name

        Parameters
        ----------
        name: str
            Data source name

        Returns
        -------
        Any
            Retrieved data source
        """
        data = cls._get(name)
        data_class = cls._data_type_to_cls_mapping[data.type]
        return data_class.get(name)

    @classmethod
    def get_by_id(cls: Any, id: ObjectId) -> Any:  # pylint: disable=redefined-builtin,invalid-name
        """
        Retrieve saved data source by ID

        Parameters
        ----------
        id: ObjectId
            Data source ID

        Returns
        -------
        Any
            Retrieved data source
        """
        data = cls._get_by_id(id)
        data_class = cls._data_type_to_cls_mapping[data.type]
        return data_class.get_by_id(id)
