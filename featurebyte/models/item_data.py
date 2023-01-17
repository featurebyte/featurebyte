"""
This module contains ItemData related models
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Optional, Type

from pydantic import validator

from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import DataModel
from featurebyte.query_graph.model.common_table import BaseTableData
from featurebyte.query_graph.model.table import ItemTableData


class ItemDataModel(ItemTableData, DataModel):
    """
    Model for ItemData entity

    id: PydanticObjectId
        Id of the object
    name : str
        Name of the ItemData
    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of ItemData columns
    status: DataStatus
        Status of the ItemData
    event_id_column: str
        Event ID column name
    item_id_column: str
        Item ID column name
    event_data_id: PydanticObjectId
        Id of the associated EventData
    created_at : Optional[datetime]
        Datetime when the ItemData was first saved or published
    updated_at: Optional[datetime]
        Datetime when the ItemData object was last updated
    """

    _table_data_class: ClassVar[Type[BaseTableData]] = ItemTableData

    @validator("record_creation_date_column")
    @classmethod
    def _check_timestamp_column_exists(
        cls, value: Optional[str], values: dict[str, Any]
    ) -> Optional[str]:
        return DataModel.validate_column_exists(
            column_name=value,
            values=values,
            expected_types={DBVarType.TIMESTAMP, DBVarType.TIMESTAMP_TZ},
        )

    @validator("event_id_column", "item_id_column")
    @classmethod
    def _check_id_column_exists(cls, value: Optional[str], values: dict[str, Any]) -> Optional[str]:
        return DataModel.validate_column_exists(
            column_name=value, values=values, expected_types={DBVarType.VARCHAR, DBVarType.INT}
        )

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.item_id_column]
