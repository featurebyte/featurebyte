"""
This module contains specialized table related models.
"""
from typing import TYPE_CHECKING, Any, ClassVar, List, Literal, Optional, Union
from typing_extensions import Annotated  # pylint: disable=wrong-import-order

from bson import ObjectId
from pydantic import Field, StrictStr, parse_obj_as, validator

from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.model.column_info import ColumnInfo, validate_columns_info
from featurebyte.query_graph.model.common_table import (
    DATA_TABLES,
    SPECIFIC_DATA_TABLES,
    BaseTableData,
    FrozenTableData,
)
from featurebyte.query_graph.node.generic import InputNode
from featurebyte.query_graph.node.schema import FeatureStoreDetails


class FrozenGenericTableData(FrozenTableData):
    """GenericTableData class"""

    type: Literal[TableDataType.GENERIC] = Field(TableDataType.GENERIC, const=True)

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": None,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            },
        )


class GenericTableData(FrozenGenericTableData, BaseTableData):
    """EventTableData class"""

    columns_info: List[ColumnInfo]  # type: ignore

    # pydantic validators
    _validator = validator("columns_info", allow_reuse=True)(validate_columns_info)

    @property
    def primary_key_columns(self) -> List[str]:
        return []


class FrozenEventTableData(FrozenTableData):
    """FrozenEventTableData class"""

    type: Literal[TableDataType.EVENT_DATA] = Field(TableDataType.EVENT_DATA, const=True)
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    event_timestamp_column: StrictStr
    event_id_column: Optional[StrictStr] = Field(default=None)  # DEV-556: this should be compulsory

    @property
    def primary_key_columns(self) -> List[str]:
        if self.event_id_column:
            return [self.event_id_column]
        return []  # DEV-556: event_id_column should not be empty

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "timestamp_column": self.event_timestamp_column,
                "id_column": self.event_id_column,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            },
        )


class EventTableData(FrozenEventTableData, BaseTableData):
    """EventTableData class"""

    columns_info: List[ColumnInfo]  # type: ignore

    # pydantic validators
    _validator = validator("columns_info", allow_reuse=True)(validate_columns_info)


class FrozenItemTableData(FrozenTableData):
    """FrozenItemTableData class"""

    type: Literal[TableDataType.ITEM_DATA] = Field(TableDataType.ITEM_DATA, const=True)
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    event_id_column: StrictStr
    item_id_column: StrictStr
    event_data_id: PydanticObjectId

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.item_id_column]

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "id_column": self.item_id_column,
                "event_data_id": self.event_data_id,
                "event_id_column": self.event_id_column,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            },
        )


class ItemTableData(FrozenItemTableData, BaseTableData):
    """ItemTableData class"""

    columns_info: List[ColumnInfo]  # type: ignore

    # pydantic validators
    _validator = validator("columns_info", allow_reuse=True)(validate_columns_info)


class FrozenDimensionTableData(FrozenTableData):
    """FrozenDimensionTableData class"""

    type: Literal[TableDataType.DIMENSION_DATA] = Field(TableDataType.DIMENSION_DATA, const=True)
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    dimension_id_column: StrictStr

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.dimension_id_column]

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "id_column": self.dimension_id_column,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            },
        )


class DimensionTableData(FrozenDimensionTableData, BaseTableData):
    """DimensionTableData class"""

    columns_info: List[ColumnInfo]  # type: ignore

    # pydantic validators
    _validator = validator("columns_info", allow_reuse=True)(validate_columns_info)


class FrozenSCDTableData(FrozenTableData):
    """FrozenSCDTableData class"""

    type: Literal[TableDataType.SCD_DATA] = Field(TableDataType.SCD_DATA, const=True)
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    natural_key_column: StrictStr
    effective_timestamp_column: StrictStr
    surrogate_key_column: Optional[StrictStr]
    end_timestamp_column: Optional[StrictStr] = Field(default=None)
    current_flag_column: Optional[StrictStr] = Field(default=None)

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.natural_key_column]

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "natural_key_column": self.natural_key_column,
                "effective_timestamp_column": self.effective_timestamp_column,
                "surrogate_key_column": self.surrogate_key_column,
                "end_timestamp_column": self.end_timestamp_column,
                "current_flag_column": self.current_flag_column,
                "feature_store_details": feature_store_details,
                **self._get_common_input_node_parameters(),
            },
        )


class SCDTableData(FrozenSCDTableData, BaseTableData):
    """SCDTableData class"""

    columns_info: List[ColumnInfo]  # type: ignore
    natural_key_column: StrictStr
    effective_timestamp_column: StrictStr
    surrogate_key_column: Optional[StrictStr]
    end_timestamp_column: Optional[StrictStr] = Field(default=None)
    current_flag_column: Optional[StrictStr] = Field(default=None)

    # pydantic validators
    _validator = validator("columns_info", allow_reuse=True)(validate_columns_info)


if TYPE_CHECKING:
    AllTableDataT = BaseTableData
    SpecificTableDataT = BaseTableData
else:
    AllTableDataT = Union[tuple(DATA_TABLES)]
    SpecificTableDataT = Annotated[Union[tuple(SPECIFIC_DATA_TABLES)], Field(discriminator="type")]


class SpecificTableData(BaseTableData):  # pylint: disable=abstract-method
    """
    Pseudo TableData class to support multiple table types.
    This class basically parses the dictionary into proper type based on its type parameter value.
    """

    def __new__(cls, **kwargs: Any) -> Any:
        return parse_obj_as(SpecificTableDataT, kwargs)  # type: ignore[misc]
