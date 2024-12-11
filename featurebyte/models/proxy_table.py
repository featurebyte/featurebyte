"""
This module contains ProxyTable pseudo models.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union

from pydantic import Field
from typing_extensions import Annotated

from featurebyte.common.model_util import construct_serialize_function
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_store import TableModel as BaseTableModel
from featurebyte.models.item_table import ItemTableModel
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.models.time_series_table import TimeSeriesTableModel

TABLE_TYPES = [
    EventTableModel,
    ItemTableModel,
    DimensionTableModel,
    SCDTableModel,
    TimeSeriesTableModel,
]
if TYPE_CHECKING:
    TableModel = BaseTableModel
else:
    TableModel = Annotated[Union[tuple(TABLE_TYPES)], Field(discriminator="type")]


class ProxyTableModel(BaseTableModel):
    """
    Pseudo Data class to support multiple table types.
    This class basically parses the persistent table model record & deserialized it into proper type.
    """

    def __new__(cls, **kwargs: Any) -> Any:
        construct_table_func = construct_serialize_function(
            all_types=TABLE_TYPES,
            annotated_type=TableModel,
            discriminator_key="type",
        )
        return construct_table_func(**kwargs)
