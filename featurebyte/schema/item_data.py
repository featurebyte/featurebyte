"""
ItemData API payload schema
"""
from typing import List, Literal

from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.tabular_data import DataCreate, DataServiceUpdate, DataUpdate


class ItemDataCreate(DataCreate):
    """
    ItemData creation schema
    """

    type: Literal[TableDataType.ITEM_DATA] = Field(TableDataType.ITEM_DATA, const=True)
    event_id_column: StrictStr
    item_id_column: StrictStr
    event_data_id: PydanticObjectId


class ItemDataList(PaginationMixin):
    """
    Paginated list of Item Data
    """

    data: List[ItemDataModel]


class ItemDataUpdate(DataUpdate):
    """
    ItemData update payload schema
    """


class ItemDataServiceUpdate(DataServiceUpdate):
    """
    ItemData service update schema
    """
