"""
This module contains ItemData related models
"""
from __future__ import annotations

from typing import ClassVar, List, Type

from pydantic import root_validator

from featurebyte.common.validator import construct_data_model_root_validator
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

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="columns_info",
            expected_column_field_name_type_pairs=[
                ("record_creation_date_column", DBVarType.supported_timestamp_types()),
                ("event_id_column", DBVarType.supported_id_types()),
                ("item_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.item_id_column]
