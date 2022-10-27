"""
This module contains ItemData related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Any, Optional

from pydantic import Field, StrictStr, validator

from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.feature_store import DataModel


class ItemDataModel(DataModel):
    """
    Model for ItemData entity

    id: PydanticObjectId
        EventData id of the object
    name : str
        Name of the EventData
    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of event data columns
    status: DataStatus
        Status of the ItemData
    event_id_column: str
        Event ID column name
    item_id_column: str
        Item ID column name
    created_at : Optional[datetime]
        Datetime when the EventData was first saved or published
    updated_at: Optional[datetime]
        Datetime when the EventData object was last updated
    """

    type: TableDataType = Field(TableDataType.ITEM_DATA, const=True)
    event_id_column: StrictStr
    item_id_column: StrictStr

    @validator("record_creation_date_column")
    @classmethod
    def _check_timestamp_column_exists(
        cls, value: Optional[str], values: dict[str, Any]
    ) -> Optional[str]:
        return DataModel.validate_column_exists(
            column_name=value, values=values, expected_types={DBVarType.TIMESTAMP}
        )

    @validator("event_id_column", "item_id_column")
    @classmethod
    def _check_id_column_exists(cls, value: Optional[str], values: dict[str, Any]) -> Optional[str]:
        return DataModel.validate_column_exists(
            column_name=value, values=values, expected_types={DBVarType.VARCHAR, DBVarType.INT}
        )

    class Settings(DataModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "item_data"
