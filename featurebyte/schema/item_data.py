"""
ItemData API payload schema
"""
from typing import List

from pydantic import StrictStr

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.tabular_data import DataCreate, DataUpdate


class ItemDataCreate(DataCreate):
    """
    ItemData creation schema
    """

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
    ItemData update schema
    """
