"""
ItemTable API payload schema
"""

from typing import List, Literal, Optional

from pydantic import StrictStr, field_validator

from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.item_table import ItemTableModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class ItemTableCreate(TableCreate):
    """
    ItemTable creation schema
    """

    type: Literal[TableDataType.ITEM_TABLE] = TableDataType.ITEM_TABLE
    event_id_column: StrictStr
    item_id_column: Optional[StrictStr]
    event_table_id: PydanticObjectId

    # pydantic validators
    _special_columns_validator = field_validator(
        "record_creation_timestamp_column",
        "event_id_column",
        "item_id_column",
        "datetime_partition_column",
    )(TableCreate._special_column_validator)


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
