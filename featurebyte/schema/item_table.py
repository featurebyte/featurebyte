"""
ItemTable API payload schema
"""
from typing import List, Literal

from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.item_table import ItemTableModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class ItemTableCreate(TableCreate):
    """
    ItemTable creation schema
    """

    type: Literal[TableDataType.ITEM_TABLE] = Field(TableDataType.ITEM_TABLE, const=True)
    event_id_column: StrictStr
    item_id_column: StrictStr
    event_table_id: PydanticObjectId


class ItemTableList(PaginationMixin):
    """
    Paginated list of ItemTable
    """

    data: List[ItemTableModel]


class ItemTableUpdate(TableUpdate):
    """
    ItemTable update payload schema
    """


class ItemTableServiceUpdate(TableServiceUpdate):
    """
    ItemTable service update schema
    """
