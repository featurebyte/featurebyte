"""
This module contains DimensionTable related models
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Tuple, Type

from pydantic import model_validator

from featurebyte.common.validator import construct_data_model_validator
from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import TableModel
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.table import DimensionTableData
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata


class DimensionTableModel(DimensionTableData, TableModel):
    """
    Model for DimensionTable entity

    dimension_id_column: str
        The primary key of the dimension table in the DWH
    """

    _table_data_class: ClassVar[Type[DimensionTableData]] = DimensionTableData

    # pydantic validators
    _model_validator = model_validator(mode="after")(
        construct_data_model_validator(
            columns_info_key="columns_info",
            expected_column_field_name_type_pairs=[
                ("record_creation_timestamp_column", DBVarType.supported_timestamp_types()),
                ("dimension_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.dimension_id_column]

    @property
    def special_columns(self) -> List[str]:
        cols = [
            self.dimension_id_column,
            self.record_creation_timestamp_column,
        ]
        return [col for col in cols if col]

    def create_view_graph_node(
        self, input_node: InputNode, metadata: ViewMetadata, **kwargs: Any
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        table_data = DimensionTableData(**self.model_dump(by_alias=True)).clone(
            column_cleaning_operations=metadata.column_cleaning_operations
        )
        return table_data.construct_dimension_view_graph_node(
            dimension_table_node=input_node,
            drop_column_names=metadata.drop_column_names,
            metadata=metadata,
        )
