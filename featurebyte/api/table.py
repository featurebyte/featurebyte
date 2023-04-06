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


class Table(TableListMixin):
    """
    A Table serves as a logical representation of a source table within the Catalog. The Table does not store data
    itself, but instead provides a way to access the source table and centralize essential metadata for feature
    engineering.

    The metadata on the table’s type determines the types of feature engineering operations that are possible and
    enforce guardrails accordingly. Additional metadata can be optionally incorporated to further aid feature
    engineering. This can include identifying columns that identify or reference entities, providing information
    about the semantics of the table columns, specifying default cleaning operations, or furnishing descriptions of
    both the table and its columns.

    At present, FeatureByte recognizes four table types:

    - Event Table
    - Item Table
    - Slowly Changing Dimension Table
    - Dimension Table.

    Two additional table types, Regular Time Series and Sensor data, will be supported in the near future.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        proxy_class="featurebyte.Table",
    )

    # class variables
    _get_schema = ProxyTableModel

    _data_type_to_cls_mapping = {
        TableDataType.EVENT_TABLE: EventTable,
        TableDataType.ITEM_TABLE: ItemTable,
        TableDataType.SCD_TABLE: SCDTable,
        TableDataType.DIMENSION_TABLE: DimensionTable,
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
        data_class = cls._data_type_to_cls_mapping[data.cached_model.type]
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
            Retrieved table source
        """
        data = cls._get_by_id(id)
        data_class = cls._data_type_to_cls_mapping[data.cached_model.type]
        return data_class.get_by_id(id)
