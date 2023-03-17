"""
Table class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.api.base_table import TableListMixin
from featurebyte.api.dimension_table import DimensionTable
from featurebyte.api.event_table import EventTable
from featurebyte.api.item_table import ItemTable
from featurebyte.api.scd_table import SCDTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.models.proxy_table import ProxyTableModel


class Table(ProxyTableModel, TableListMixin):
    """
    Table class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["Table"],
        proxy_class="featurebyte.Table",
    )

    # class variables
    _get_schema = ProxyTableModel

    _data_type_to_cls_mapping = {
        TableDataType.EVENT_DATA: EventTable,
        TableDataType.ITEM_DATA: ItemTable,
        TableDataType.SCD_DATA: SCDTable,
        TableDataType.DIMENSION_DATA: DimensionTable,
    }

    @classmethod
    def get(cls: Any, name: str) -> Any:
        """
        Retrieve saved source table by name

        Parameters
        ----------
        name: str
            Source table name

        Returns
        -------
        Any
            Retrieved source table
        """
        data = cls._get(name)
        data_class = cls._data_type_to_cls_mapping[data.type]
        return data_class.get(name)

    @classmethod
    def get_by_id(cls: Any, id: ObjectId) -> Any:  # pylint: disable=redefined-builtin,invalid-name
        """
        Retrieve saved source table by ID

        Parameters
        ----------
        id: ObjectId
            Source Table ID

        Returns
        -------
        Any
            Retrieved data source
        """
        data = cls._get_by_id(id)
        data_class = cls._data_type_to_cls_mapping[data.type]
        return data_class.get_by_id(id)
