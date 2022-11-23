"""
This module contains SQL operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, Dict, List, Literal, Optional, Set, Union
from typing_extensions import Annotated

from pydantic import BaseModel, Field, root_validator

from featurebyte.enum import AggFunc, TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreDetails, TableDetails
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode, BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.column import InColumnStr, OutColumnStr
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    NodeOutputCategory,
    OperationStructure,
    PostAggregationColumn,
    SourceDataColumn,
    ViewDataColumn,
)
from featurebyte.query_graph.node.mixin import GroupbyNodeOpStructMixin


class InputNode(BaseNode):
    """InputNode class"""

    class BaseParameters(BaseModel):
        """BaseParameters"""

        columns: List[InColumnStr]
        table_details: TableDetails
        feature_store_details: FeatureStoreDetails

    class GenericParameters(BaseParameters):
        """GenericParameters"""

        type: Literal[TableDataType.GENERIC] = Field(TableDataType.GENERIC)
        id: Optional[PydanticObjectId] = Field(default=None)

    class EventDataParameters(BaseParameters):
        """EventDataParameters"""

        type: Literal[TableDataType.EVENT_DATA] = Field(TableDataType.EVENT_DATA, const=True)
        timestamp: Optional[InColumnStr]
        id: Optional[PydanticObjectId] = Field(default=None)

        @root_validator(pre=True)
        @classmethod
        def _convert_node_parameters_format(cls, values: Dict[str, Any]) -> Dict[str, Any]:
            # DEV-556: converted older record (parameters) into a newer format
            if "dbtable" in values:
                values["table_details"] = values["dbtable"]
            if "feature_store" in values:
                values["feature_store_details"] = values["feature_store"]
            return values

    class ItemDataParameters(BaseParameters):
        """ItemDataParameters"""

        type: Literal[TableDataType.ITEM_DATA] = Field(TableDataType.ITEM_DATA, const=True)
        id: Optional[PydanticObjectId] = Field(default=None)

    class DimensionDataParameters(BaseParameters):
        """DimensionDataParameters"""

        type: Literal[TableDataType.DIMENSION_DATA] = Field(
            TableDataType.DIMENSION_DATA, const=True
        )
        id: Optional[PydanticObjectId] = Field(default=None)

    class SCDDataParameters(BaseParameters):
        """SCDDataParameters"""

        type: Literal[TableDataType.SCD_DATA] = Field(TableDataType.SCD_DATA, const=True)
        id: Optional[PydanticObjectId] = Field(default=None)

    type: Literal[NodeType.INPUT] = Field(NodeType.INPUT, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Annotated[
        Union[
            EventDataParameters,
            ItemDataParameters,
            GenericParameters,
            DimensionDataParameters,
            SCDDataParameters,
        ],
        Field(discriminator="type"),
    ]

    @root_validator(pre=True)
    @classmethod
    def _set_default_table_data_type(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # DEV-556: set default table data type when it is not present
        if "parameters" in values:
            if "type" not in values["parameters"]:
                values["parameters"]["type"] = TableDataType.EVENT_DATA
        return values

    def derive_node_operation_info(
        self, inputs: List[OperationStructure], visited_node_types: Set[NodeType]
    ) -> OperationStructure:
        _ = visited_node_types
        return OperationStructure(
            columns=[
                SourceDataColumn(
                    name=column,
                    tabular_data_id=self.parameters.id,
                    tabular_data_type=self.parameters.type,
                )
                for column in self.parameters.columns
            ],
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
        )


class ProjectNode(BaseNode):
    """ProjectNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        columns: List[InColumnStr]

    type: Literal[NodeType.PROJECT] = Field(NodeType.PROJECT, const=True)
    parameters: Parameters

    def derive_node_operation_info(
        self, inputs: List[OperationStructure], visited_node_types: Set[NodeType]
    ) -> OperationStructure:
        _ = visited_node_types
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category
        names = set(self.get_required_input_columns())
        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                col for col in input_operation_info.columns if col.name in names
            ]
        else:
            node_kwargs["columns"] = input_operation_info.columns
            node_kwargs["aggregations"] = [
                col for col in input_operation_info.aggregations if col.name in names
            ]

        return OperationStructure(
            **node_kwargs, output_type=self.output_type, output_category=output_category
        )


class FilterNode(BaseNode):
    """FilterNode class"""

    type: Literal[NodeType.FILTER] = Field(NodeType.FILTER, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)

    def derive_node_operation_info(
        self, inputs: List[OperationStructure], visited_node_types: Set[NodeType]
    ) -> OperationStructure:
        _ = visited_node_types
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category
        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                type(col)(**{**col.dict(), "filter": True}) for col in input_operation_info.columns
            ]
        else:
            node_kwargs["columns"] = input_operation_info.columns
            node_kwargs["aggregations"] = [
                PostAggregationColumn.create(
                    name=col.name, columns=[col], transform=self.transform_info
                )
                for col in input_operation_info.aggregations
            ]

        return OperationStructure(
            **node_kwargs,
            output_type=input_operation_info.output_type,
            output_category=output_category,
        )


class AssignNode(BaseNode):
    """AssignNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        name: OutColumnStr
        value: Optional[Any]

    type: Literal[NodeType.ASSIGN] = Field(NodeType.ASSIGN, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    def derive_node_operation_info(
        self, inputs: List[OperationStructure], visited_node_types: Set[NodeType]
    ) -> OperationStructure:
        _ = visited_node_types
        input_operation_info, series_operation_info = inputs
        new_column_name = self.parameters.name
        new_column = DerivedDataColumn.create(
            name=new_column_name,
            columns=series_operation_info.columns,
            transform=None,
        )
        return OperationStructure(
            columns=input_operation_info.columns + [new_column],
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
        )


class LagNode(BaseSeriesOutputNode):
    """LagNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        entity_columns: List[InColumnStr]
        timestamp_column: InColumnStr
        offset: int

    type: Literal[NodeType.LAG] = Field(NodeType.LAG, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: Parameters


class GroupbyNode(GroupbyNodeOpStructMixin, BaseNode):
    """GroupbyNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        keys: List[InColumnStr]
        parent: Optional[InColumnStr]
        agg_func: AggFunc
        value_by: Optional[InColumnStr]
        windows: List[str]
        timestamp: InColumnStr
        blind_spot: int
        time_modulo_frequency: int
        frequency: int
        names: List[str]  # do not use `OutColumnStr` here as the output is feature but not view
        serving_names: List[str]
        tile_id: Optional[str]
        aggregation_id: Optional[str]
        entity_ids: Optional[List[PydanticObjectId]]

    type: Literal[NodeType.GROUPBY] = Field(NodeType.GROUPBY, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    def _exclude_source_columns(self) -> List[str]:
        cols = self.parameters.keys + [self.parameters.timestamp]
        return [str(col) for col in cols]

    def _get_aggregations(self, columns: List[ViewDataColumn]) -> List[AggregationColumn]:
        col_name_map = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=name,
                method=self.parameters.agg_func,
                groupby=self.parameters.keys,
                window=window,
                category=self.parameters.value_by,
                column=col_name_map.get(self.parameters.parent),
                filter=any(col.filter for col in columns),
                groupby_type=self.type,
            )
            for name, window in zip(self.parameters.names, self.parameters.windows)
        ]


class ItemGroupbyNode(GroupbyNodeOpStructMixin, BaseNode):
    """ItemGroupbyNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        keys: List[InColumnStr]
        parent: Optional[InColumnStr]
        agg_func: AggFunc
        names: List[str]
        serving_names: List[str]
        entity_ids: Optional[List[PydanticObjectId]]

    type: Literal[NodeType.ITEM_GROUPBY] = Field(NodeType.ITEM_GROUPBY, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    def _exclude_source_columns(self) -> List[str]:
        return [str(key) for key in self.parameters.keys]

    def _get_aggregations(self, columns: List[ViewDataColumn]) -> List[AggregationColumn]:
        col_name_map = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=name,
                method=self.parameters.agg_func,
                groupby=self.parameters.keys,
                window=None,
                category=None,
                column=col_name_map.get(self.parameters.parent),
                filter=any(col.filter for col in columns),
                groupby_type=self.type,
            )
            for name in self.parameters.names
        ]


class JoinNode(BaseNode):
    """Join class"""

    class Parameters(BaseModel):
        """Parameters"""

        left_on: str
        right_on: str
        left_input_columns: List[InColumnStr]
        left_output_columns: List[OutColumnStr]
        right_input_columns: List[InColumnStr]
        right_output_columns: List[OutColumnStr]
        join_type: Literal["left", "inner"]

    type: Literal[NodeType.JOIN] = Field(NodeType.JOIN, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    def derive_node_operation_info(
        self, inputs: List[OperationStructure], visited_node_types: Set[NodeType]
    ) -> OperationStructure:
        _ = visited_node_types
        left_output_columns = set(self.parameters.left_output_columns)
        right_output_columns = set(self.parameters.right_output_columns)
        left_columns = [col for col in inputs[0].columns if col.name in left_output_columns]
        right_columns = [col for col in inputs[1].columns if col.name in right_output_columns]
        return OperationStructure(
            columns=left_columns + right_columns,
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
        )


class AliasNode(BaseNode):
    """AliasNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        name: OutColumnStr

    type: Literal[NodeType.ALIAS] = Field(NodeType.ALIAS, const=True)
    parameters: Parameters

    def derive_node_operation_info(
        self, inputs: List[OperationStructure], visited_node_types: Set[NodeType]
    ) -> OperationStructure:
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category

        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            last_column = input_operation_info.columns[-1]
            new_last_column = type(last_column)(
                **{**last_column.dict(), "name": self.parameters.name}
            )
            node_kwargs["columns"] = list(input_operation_info.columns)
            node_kwargs["columns"][-1] = new_last_column
        else:
            last_aggregation = input_operation_info.aggregations[-1]
            new_last_aggregation = type(last_aggregation)(
                **{**last_aggregation.dict(), "name": self.parameters.name}
            )
            node_kwargs["columns"] = input_operation_info.columns
            node_kwargs["aggregations"] = list(input_operation_info.aggregations)
            node_kwargs["aggregations"][-1] = new_last_aggregation

        return OperationStructure(
            **node_kwargs, output_type=self.output_type, output_category=output_category
        )
