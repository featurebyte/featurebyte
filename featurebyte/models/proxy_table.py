"""
This module contains ProxyTable pseudo models.
"""

from __future__ import annotations

from typing import Any, Union
from typing_extensions import Annotated

from pydantic import Field, TypeAdapter

from featurebyte.enum import TableDataType
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_store import TableModel as BaseTableModel
from featurebyte.models.item_table import ItemTableModel
from featurebyte.models.scd_table import SCDTableModel

TableModel = Annotated[
    Union[EventTableModel, ItemTableModel, DimensionTableModel, SCDTableModel],
    Field(discriminator="type"),
]
TABLE_CLASS_MAP = {
    TableDataType.EVENT_TABLE.value: EventTableModel,
    TableDataType.ITEM_TABLE.value: ItemTableModel,
    TableDataType.DIMENSION_TABLE.value: DimensionTableModel,
    TableDataType.SCD_TABLE.value: SCDTableModel,
}


class ProxyTableModel(BaseTableModel):  # pylint: disable=abstract-method
    """
    Pseudo Data class to support multiple table types.
    This class basically parses the persistent table model record & deserialized it into proper type.
    """

    def __new__(cls, **kwargs: Any) -> Any:
        table_class = TABLE_CLASS_MAP.get(kwargs.get("type"))
        if table_class is None:
            # use pydantic builtin version to throw validation error (slow due to pydantic V2 performance issue)
            return TypeAdapter(TableModel).validate_python(kwargs)

        # use internal method to avoid current pydantic V2 performance issue due to _core_utils.py:walk
        # https://github.com/pydantic/pydantic/issues/6768
        return table_class(**kwargs)  # type: ignore
