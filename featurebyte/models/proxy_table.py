"""
This module contains ProxyTable pseudo models.
"""

from __future__ import annotations

from typing import Any, Union
from typing_extensions import Annotated

from pydantic import Field, parse_obj_as

from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_store import TableModel as BaseTableModel
from featurebyte.models.item_table import ItemTableModel
from featurebyte.models.scd_table import SCDTableModel

TableModel = Annotated[
    Union[EventTableModel, ItemTableModel, DimensionTableModel, SCDTableModel],
    Field(discriminator="type"),
]


class ProxyTableModel(BaseTableModel):  # pylint: disable=abstract-method
    """
    Pseudo Data class to support multiple table types.
    This class basically parses the persistent table model record & deserialized it into proper type.
    """

    def __new__(cls, **kwargs: Any) -> Any:
        return parse_obj_as(TableModel, kwargs)  # type: ignore[arg-type]
