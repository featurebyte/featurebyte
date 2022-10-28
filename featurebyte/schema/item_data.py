"""
ItemData API payload schema
"""
from typing import List

from pydantic import StrictStr

from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.data import DataCreate, DataUpdate


class ItemDataCreate(DataCreate):
    """
    ItemData creation schema
    """

    event_id_column: StrictStr
    item_id_column: StrictStr


class ItemDataList(PaginationMixin):
    """
    Paginated list of Item Data
    """

    data: List[ItemDataModel]


class ItemDataUpdate(DataUpdate):
    """
    ItemData update schema
    """
