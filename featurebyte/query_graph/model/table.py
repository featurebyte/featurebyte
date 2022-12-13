"""
This module contains specialized table related models.
"""
from typing import Any, Dict, List, Literal

from abc import abstractmethod

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import BaseTableData, TabularSource
from featurebyte.query_graph.model.feature_store import FeatureStoreDetails
from featurebyte.query_graph.node.generic import InputNode


class ConstructNodeMixin:
    """GetInputNodeMixin class"""

    type: Literal[
        TableDataType.GENERIC,
        TableDataType.EVENT_DATA,
        TableDataType.ITEM_DATA,
        TableDataType.DIMENSION_DATA,
        TableDataType.SCD_DATA,
    ]
    columns_info: List[ColumnInfo]
    tabular_source: TabularSource

    def _get_common_input_node_parameters(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "columns": [col.name for col in self.columns_info],
            "table_details": self.tabular_source.table_details,
        }

    @abstractmethod
    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        """
        Construct input node based on data info

        Parameters
        ----------
        feature_store_details: FeatureStoreDetails
            Feature store details

        Returns
        -------
        InputNode
        """


class GenericTableData(ConstructNodeMixin, BaseTableData):
    """GenericTableData class"""

    type: Literal[TableDataType.GENERIC] = Field(TableDataType.GENERIC, const=True)

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            parameters={
                "id": None,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            }
        )


class EventTableData(ConstructNodeMixin, BaseTableData):
    """EventTableData class"""

    type: Literal[TableDataType.EVENT_DATA] = Field(TableDataType.EVENT_DATA, const=True)
    event_timestamp_column: StrictStr
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            parameters={
                "id": self.id,
                "timestamp": self.event_timestamp_column,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            }
        )


class ItemTableData(ConstructNodeMixin, BaseTableData):
    """ItemTableData class"""

    type: Literal[TableDataType.ITEM_DATA] = Field(TableDataType.ITEM_DATA, const=True)
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            parameters={
                "id": self.id,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            }
        )


class DimensionTableData(ConstructNodeMixin, BaseTableData):
    """DimensionTableData class"""

    type: Literal[TableDataType.DIMENSION_DATA] = Field(TableDataType.DIMENSION_DATA, const=True)
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            parameters={
                "id": self.id,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            }
        )


class SCDTableData(ConstructNodeMixin, BaseTableData):
    """SCDTableData class"""

    type: Literal[TableDataType.SCD_DATA] = Field(TableDataType.SCD_DATA, const=True)
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            parameters={
                "id": self.id,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            }
        )
