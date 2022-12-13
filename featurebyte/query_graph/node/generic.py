"""
This module contains SQL operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, Dict, List, Literal, Optional, Sequence, Set, Union
from typing_extensions import Annotated

from pydantic import BaseModel, Field, root_validator

from featurebyte.enum import AggFunc, TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreDetails
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.table import TableDetails
from featurebyte.query_graph.node.base import (
    BaseNode,
    BasePrunableNode,
    BaseSeriesOutputNode,
    NodeT,
)
from featurebyte.query_graph.node.metadata.column import InColumnStr, OutColumnStr
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    NodeOutputCategory,
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
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

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        _ = branch_state, global_state
        return OperationStructure(
            columns=[
                SourceDataColumn(
                    name=column,
                    tabular_data_id=self.parameters.id,
                    tabular_data_type=self.parameters.type,
                    node_names={self.name},
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

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        _ = branch_state, global_state
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category
        names = set(self.get_required_input_columns())
        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                col.clone(node_names=col.node_names.union([self.name]))
                for col in input_operation_info.columns
                if col.name in names
            ]
        else:
            node_kwargs["columns"] = input_operation_info.columns
            node_kwargs["aggregations"] = [
                col.clone(node_names=col.node_names.union([self.name]))
                for col in input_operation_info.aggregations
                if col.name in names
            ]

        return OperationStructure(
            **node_kwargs, output_type=self.output_type, output_category=output_category
        )


class FilterNode(BaseNode):
    """FilterNode class"""

    type: Literal[NodeType.FILTER] = Field(NodeType.FILTER, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        _ = branch_state, global_state
        input_operation_info, mask_operation_info = inputs
        output_category = input_operation_info.output_category
        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            other_node_names = {self.name}.union(mask_operation_info.all_node_names)
            node_kwargs["columns"] = [
                col.clone(filter=True, node_names=col.node_names.union(other_node_names))
                for col in input_operation_info.columns
            ]
        else:
            node_kwargs["columns"] = input_operation_info.columns
            node_kwargs["aggregations"] = [
                PostAggregationColumn.create(
                    name=col.name,
                    columns=[col],
                    transform=self.transform_info,
                    node_name=self.name,
                    other_node_names=mask_operation_info.all_node_names,
                )
                for col in input_operation_info.aggregations
            ]

        return OperationStructure(
            **node_kwargs,
            output_type=input_operation_info.output_type,
            output_category=output_category,
        )


class AssignColumnMixin:
    """AssignColumnMixin class"""

    def resolve_node_pruned(self, input_node_names: List[str]) -> str:
        """
        Method used to resolve the situation when the node get pruned. As all the nodes only produce single
        output, we should only choose one node from the input nodes.

        Parameters
        ----------
        input_node_names: List[str]
            List of input node names

        Returns
        -------
        str
            Node name selected to replace this (pruned) node
        """
        # for the assign-like operation, the first column is a frame view
        # if the node is pruned (mean that the new column is not required),
        # we should use the frame input to resolve the situation.
        return input_node_names[0]

    @staticmethod
    def _validate_view(view_op_structure: OperationStructure) -> None:
        assert view_op_structure.output_category == NodeOutputCategory.VIEW
        assert view_op_structure.output_type == NodeOutputType.FRAME

    @staticmethod
    def _construct_operation_structure(
        input_operation_info: OperationStructure,
        new_column_name: str,
        columns: List[ViewDataColumn],
        node_name: str,
    ) -> OperationStructure:
        """
        Construct operation structure of the assign-column-like operation

        Parameters
        ----------
        input_operation_info: OperationStructure
            Input operation info of a frame that a series will be assigned to
        new_column_name: str
            Column name that will be assigned to the new column (from the series input)
        columns: List[ViewDataColumn]
            List of columns that are used to derive the series
        node_name: str
            Node name of the operation that assign a series to a frame

        Returns
        -------
        OperationStructure
        """
        input_columns = [col for col in input_operation_info.columns if col.name != new_column_name]
        new_column = DerivedDataColumn.create(
            name=new_column_name,
            columns=columns,
            transform=None,
            node_name=node_name,
        )
        return OperationStructure(
            columns=input_columns + [new_column],
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
        )


class AssignNode(AssignColumnMixin, BasePrunableNode):
    """AssignNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        name: OutColumnStr
        value: Optional[Any]

    type: Literal[NodeType.ASSIGN] = Field(NodeType.ASSIGN, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    @staticmethod
    def _validate_series(series_op_structure: OperationStructure) -> None:
        assert series_op_structure.output_type == NodeOutputType.SERIES
        assert series_op_structure.output_category == NodeOutputCategory.VIEW

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:

        # First input is a View
        input_operation_info = inputs[0]
        self._validate_view(input_operation_info)

        # AssignNode can only take 1 or 2 parameters (1 parameters is frame, 2nd optional parameter is series)
        columns = []
        if len(inputs) == 2:
            columns = inputs[1].columns
            self._validate_series(inputs[1])

        return self._construct_operation_structure(
            input_operation_info=input_operation_info,
            new_column_name=self.parameters.name,
            columns=columns,
            node_name=self.name,
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
        names: List[OutColumnStr]
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

    def _get_aggregations(
        self, columns: List[ViewDataColumn], node_name: str, other_node_names: Set[str]
    ) -> List[AggregationColumn]:
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
                node_names={node_name}.union(other_node_names),
            )
            for name, window in zip(self.parameters.names, self.parameters.windows)
        ]

    def prune(self: NodeT, target_nodes: Sequence[NodeT]) -> NodeT:
        # Only prune the groupby node if all the target output are the project node, this is to prevent
        # unexpected parameters pruning if groupby node output is used by other node like graph node.
        if target_nodes and all(node.type == NodeType.PROJECT for node in target_nodes):
            required_columns = set().union(
                *(node.get_required_input_columns() for node in target_nodes)
            )
            params = self.parameters
            pruned_params_dict = self.parameters.dict()
            pruned_params_dict.update(names=[], windows=[])
            for name, window in zip(params.names, params.windows):  # type: ignore
                if name in required_columns:
                    pruned_params_dict["names"].append(name)
                    pruned_params_dict["windows"].append(window)
            return self.clone(parameters=pruned_params_dict)
        return self


class ItemGroupbyNode(GroupbyNodeOpStructMixin, BaseNode):
    """ItemGroupbyNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        keys: List[InColumnStr]
        parent: Optional[InColumnStr]
        agg_func: AggFunc
        value_by: Optional[InColumnStr]
        name: OutColumnStr
        serving_names: List[str]
        entity_ids: Optional[List[PydanticObjectId]]

    type: Literal[NodeType.ITEM_GROUPBY] = Field(NodeType.ITEM_GROUPBY, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    def _exclude_source_columns(self) -> List[str]:
        return [str(key) for key in self.parameters.keys]

    def _get_aggregations(
        self, columns: List[ViewDataColumn], node_name: str, other_node_names: Set[str]
    ) -> List[AggregationColumn]:
        col_name_map = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=self.parameters.name,
                method=self.parameters.agg_func,
                groupby=self.parameters.keys,
                window=None,
                category=None,
                column=col_name_map.get(self.parameters.parent),
                filter=any(col.filter for col in columns),
                groupby_type=self.type,
                node_names={node_name}.union(other_node_names),
            )
        ]


class SCDParameters(BaseModel):
    """Parameters specific to SCD joins"""

    left_timestamp_column: str
    right_timestamp_column: str
    current_flag: Optional[str]
    end_timestamp_column: Optional[str]


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
        scd_parameters: Optional[SCDParameters]

    type: Literal[NodeType.JOIN] = Field(NodeType.JOIN, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        params = self.parameters
        left_col_map = dict(zip(params.left_input_columns, params.left_output_columns))
        right_col_map = dict(zip(params.right_input_columns, params.right_output_columns))
        left_columns = [
            col.clone(name=left_col_map[col.name], node_names=col.node_names.union([self.name]))  # type: ignore
            for col in inputs[0].columns
            if col.name in left_col_map
        ]
        right_columns = [
            col.clone(name=right_col_map[col.name], node_names=col.node_names.union([self.name]))  # type: ignore
            for col in inputs[1].columns
            if col.name in right_col_map
        ]
        return OperationStructure(
            columns=left_columns + right_columns,
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
        )


class JoinFeatureNode(AssignColumnMixin, BasePrunableNode):
    """JoinFeatureNode class

    This node should have two input nodes. The first input node is the View's node, and the second
    input node is the Feature's node.
    """

    class Parameters(BaseModel):
        """
        Parameters for JoinFeatureNode

        Parameters
        ----------
        view_entity_column: str
            Column name in the View to be used as join key
        view_point_in_time_column: Optional[str]
            Column name in the View to be used as point in time column when joining with a time
            based feature
        feature_entity_column: str
            Join key for the feature. For non-time based features, this should be the key parameter
            of the ItemGroupbyNode that generated the feature
        name: str
            Name of the column when the feature is added to the EventView
        """

        view_entity_column: InColumnStr
        view_point_in_time_column: Optional[InColumnStr]
        feature_entity_column: InColumnStr
        name: OutColumnStr

    type: Literal[NodeType.JOIN_FEATURE] = Field(NodeType.JOIN_FEATURE, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    @staticmethod
    def _validate_feature(feature_op_structure: OperationStructure) -> None:
        columns = feature_op_structure.columns
        assert len(columns) == 1
        # For now, the supported feature should have an item_groupby node in its lineage
        assert any(node_name.startswith("item_groupby") for node_name in columns[0].node_names)
        assert feature_op_structure.output_type == NodeOutputType.SERIES

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:

        # First input is a View
        input_operation_info = inputs[0]
        self._validate_view(input_operation_info)

        # Second input node is a Feature
        feature_operation_info = inputs[1]
        self._validate_feature(feature_operation_info)

        # If this View has a column that has the same name as the feature to be added, it will be
        # omitted. This is because the added feature will replace that existing column.
        return self._construct_operation_structure(
            input_operation_info=input_operation_info,
            new_column_name=self.parameters.name,
            columns=feature_operation_info.columns,
            node_name=self.name,
        )


class AliasNode(BaseNode):
    """AliasNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        name: OutColumnStr

    type: Literal[NodeType.ALIAS] = Field(NodeType.ALIAS, const=True)
    parameters: Parameters

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        _ = branch_state, global_state
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category

        node_kwargs: Dict[str, Any] = {}
        new_name = self.parameters.name
        if output_category == NodeOutputCategory.VIEW:
            last_col = input_operation_info.columns[-1]
            node_kwargs["columns"] = list(input_operation_info.columns)
            node_kwargs["columns"][-1] = last_col.clone(
                name=new_name, node_names=last_col.node_names.union([self.name])
            )
        else:
            last_agg = input_operation_info.aggregations[-1]
            node_kwargs["columns"] = input_operation_info.columns
            node_kwargs["aggregations"] = list(input_operation_info.aggregations)
            node_kwargs["aggregations"][-1] = last_agg.clone(
                name=new_name, node_names=last_agg.node_names.union([self.name])
            )

        return OperationStructure(
            **node_kwargs, output_type=self.output_type, output_category=output_category
        )
