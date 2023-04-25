"""
This module contains ItemTable related models
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Tuple, Type

from pydantic import root_validator

from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import TableModel
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.table import ItemTableData
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ItemViewMetadata


class ItemTableModel(ItemTableData, TableModel):
    """
    Model for ItemTable entity

    id: PydanticObjectId
        Id of the object
    name : str
        Name of the ItemTable
    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of ItemTable columns
    status: TableStatus
        Status of the ItemTable
    event_id_column: str
        Event ID column name
    item_id_column: str
        Item ID column name
    event_table_id: PydanticObjectId
        Id of the associated EventTable
    created_at : Optional[datetime]
        Datetime when the ItemTable was first saved or published
    updated_at: Optional[datetime]
        Datetime when the ItemTable object was last updated
    """

    _table_data_class: ClassVar[Type[ItemTableData]] = ItemTableData

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="columns_info",
            expected_column_field_name_type_pairs=[
                ("record_creation_timestamp_column", DBVarType.supported_timestamp_types()),
                ("event_id_column", DBVarType.supported_id_types()),
                ("item_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.item_id_column]

    def create_view_graph_node(
        self,
        input_node: InputNode,
        metadata: ItemViewMetadata,
        **kwargs: Any,
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        table_data = ItemTableData(**self.dict(by_alias=True)).clone(
            column_cleaning_operations=metadata.column_cleaning_operations
        )
        (
            view_graph_node,
            columns_info,
        ) = table_data.construct_item_view_graph_node(  # pylint: disable=no-member
            item_table_node=input_node,
            columns_to_join=metadata.event_join_column_names,
            event_suffix=metadata.event_suffix,
            drop_column_names=metadata.drop_column_names,
            metadata=metadata,
            **kwargs,
        )
        return view_graph_node, columns_info
