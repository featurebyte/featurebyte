"""
This module contains SQL operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, Dict, List, Literal, Optional, Sequence, Set, Union
from typing_extensions import Annotated

from pydantic import BaseModel, Field, root_validator, validator

from featurebyte.enum import AggFunc, DBVarType, TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
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
from featurebyte.query_graph.node.mixin import AggregationOpStructMixin
from featurebyte.query_graph.node.schema import ColumnSpec, FeatureStoreDetails, TableDetails
from featurebyte.query_graph.util import append_to_lineage


class InputNode(BaseNode):
    """InputNode class"""

    class BaseParameters(BaseModel):
        """BaseParameters"""

        columns: List[ColumnSpec]
        table_details: TableDetails
        feature_store_details: FeatureStoreDetails

        @root_validator(pre=True)
        @classmethod
        def _convert_columns_format(cls, values: Dict[str, Any]) -> Dict[str, Any]:
            # DEV-556: convert list of string to list of dictionary
            columns = values.get("columns")
            if columns and isinstance(columns[0], str):
                values["columns"] = [{"name": col, "dtype": DBVarType.UNKNOWN} for col in columns]
            return values

    class GenericParameters(BaseParameters):
        """GenericParameters"""

        type: Literal[TableDataType.GENERIC] = Field(TableDataType.GENERIC)
        id: Optional[PydanticObjectId] = Field(default=None)

    class EventDataParameters(BaseParameters):
        """EventDataParameters"""

        type: Literal[TableDataType.EVENT_DATA] = Field(TableDataType.EVENT_DATA, const=True)
        id: Optional[PydanticObjectId] = Field(default=None)
        timestamp_column: Optional[InColumnStr] = Field(
            default=None
        )  # DEV-556: this should be compulsory
        id_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: this should be compulsory

        @root_validator(pre=True)
        @classmethod
        def _convert_node_parameters_format(cls, values: Dict[str, Any]) -> Dict[str, Any]:
            # DEV-556: converted older record (parameters) into a newer format
            if "dbtable" in values:
                values["table_details"] = values["dbtable"]
            if "feature_store" in values:
                values["feature_store_details"] = values["feature_store"]
            if "timestamp" in values:
                values["timestamp_column"] = values["timestamp"]
            return values

    class ItemDataParameters(BaseParameters):
        """ItemDataParameters"""

        type: Literal[TableDataType.ITEM_DATA] = Field(TableDataType.ITEM_DATA, const=True)
        id: Optional[PydanticObjectId] = Field(default=None)
        id_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: this should be compulsory
        event_data_id: Optional[PydanticObjectId] = Field(default=None)
        event_id_column: Optional[InColumnStr] = Field(default=None)

    class DimensionDataParameters(BaseParameters):
        """DimensionDataParameters"""

        type: Literal[TableDataType.DIMENSION_DATA] = Field(
            TableDataType.DIMENSION_DATA, const=True
        )
        id: Optional[PydanticObjectId] = Field(default=None)
        id_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: this should be compulsory

    class SCDDataParameters(BaseParameters):
        """SCDDataParameters"""

        type: Literal[TableDataType.SCD_DATA] = Field(TableDataType.SCD_DATA, const=True)
        id: Optional[PydanticObjectId] = Field(default=None)
        natural_key_column: Optional[InColumnStr] = Field(
            default=None
        )  # DEV-556: this should be compulsory
        effective_timestamp_column: Optional[InColumnStr] = Field(
            default=None
        )  # DEV-556: this should be compulsory
        surrogate_key_column: Optional[InColumnStr] = Field(default=None)
        end_timestamp_column: Optional[InColumnStr] = Field(default=None)
        current_flag_column: Optional[InColumnStr] = Field(default=None)

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
                    name=column.name,
                    tabular_data_id=self.parameters.id,
                    tabular_data_type=self.parameters.type,
                    node_names={self.name},
                    node_name=self.name,
                    dtype=column.dtype,
                )
                for column in self.parameters.columns
            ],
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=(self.name,),
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
            **node_kwargs,
            output_type=self.output_type,
            output_category=output_category,
            row_index_lineage=input_operation_info.row_index_lineage,
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
                col.clone(
                    filter=True,
                    node_names=col.node_names.union(other_node_names),
                    node_name=self.name,
                )
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
                    dtype=col.dtype,
                )
                for col in input_operation_info.aggregations
            ]

        return OperationStructure(
            **node_kwargs,
            output_type=input_operation_info.output_type,
            output_category=output_category,
            row_index_lineage=append_to_lineage(input_operation_info.row_index_lineage, self.name),
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
        new_column_var_type: DBVarType,
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
        new_column_var_type: DBVarType
            Variable type of new column

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
            dtype=new_column_var_type,
        )
        return OperationStructure(
            columns=input_columns + [new_column],
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=input_operation_info.row_index_lineage,
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
            series_input = inputs[1]
            self._validate_series(series_input)
            columns = series_input.columns
            dtype = series_input.series_output_dtype
        else:
            dtype = self.detect_var_type_from_value(self.parameters.value)

        return self._construct_operation_structure(
            input_operation_info=input_operation_info,
            new_column_name=self.parameters.name,
            columns=columns,
            node_name=self.name,
            new_column_var_type=dtype,
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

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return inputs[0].series_output_dtype


class BaseGroupbyParameters(BaseModel):
    """Common parameters related to groupby operation"""

    keys: List[InColumnStr]
    parent: Optional[InColumnStr]
    agg_func: AggFunc
    value_by: Optional[InColumnStr]
    serving_names: List[str]
    entity_ids: Optional[List[PydanticObjectId]]


class GroupbyNode(AggregationOpStructMixin, BaseNode):
    """GroupbyNode class"""

    class Parameters(BaseGroupbyParameters):
        """Parameters"""

        windows: List[Optional[str]]
        timestamp: InColumnStr
        blind_spot: int
        time_modulo_frequency: int
        frequency: int
        names: List[OutColumnStr]
        tile_id: Optional[str]
        aggregation_id: Optional[str]

    type: Literal[NodeType.GROUPBY] = Field(NodeType.GROUPBY, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    def _exclude_source_columns(self) -> List[str]:
        cols = self.parameters.keys + [self.parameters.timestamp]
        return [str(col) for col in cols]

    def _get_aggregations(
        self,
        columns: List[ViewDataColumn],
        node_name: str,
        other_node_names: Set[str],
        output_var_type: DBVarType,
    ) -> List[AggregationColumn]:
        col_name_map = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=name,
                method=self.parameters.agg_func,
                keys=self.parameters.keys,
                window=window,
                category=self.parameters.value_by,
                column=col_name_map.get(self.parameters.parent),
                filter=any(col.filter for col in columns),
                aggregation_type=self.type,
                node_names={node_name}.union(other_node_names),
                node_name=node_name,
                dtype=output_var_type,
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


class ItemGroupbyNode(AggregationOpStructMixin, BaseNode):
    """ItemGroupbyNode class"""

    class Parameters(BaseGroupbyParameters):
        """Parameters"""

        name: OutColumnStr

    type: Literal[NodeType.ITEM_GROUPBY] = Field(NodeType.ITEM_GROUPBY, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    def _exclude_source_columns(self) -> List[str]:
        return [str(key) for key in self.parameters.keys]

    def _get_aggregations(
        self,
        columns: List[ViewDataColumn],
        node_name: str,
        other_node_names: Set[str],
        output_var_type: DBVarType,
    ) -> List[AggregationColumn]:
        col_name_map = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=self.parameters.name,
                method=self.parameters.agg_func,
                keys=self.parameters.keys,
                window=None,
                category=None,
                column=col_name_map.get(self.parameters.parent),
                filter=any(col.filter for col in columns),
                aggregation_type=self.type,
                node_names={node_name}.union(other_node_names),
                node_name=node_name,
                dtype=output_var_type,
            )
        ]


class SCDBaseParameters(BaseModel):
    """Parameters common to SCD data"""

    effective_timestamp_column: InColumnStr
    natural_key_column: Optional[InColumnStr] = Field(default=None)  # DEV-556: should be compulsory
    current_flag_column: Optional[InColumnStr]
    end_timestamp_column: Optional[InColumnStr]

    @root_validator(pre=True)
    @classmethod
    def _convert_node_parameters_format(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # DEV-556: backward compatibility
        if "right_timestamp_column" in values:
            values["effective_timestamp_column"] = values["right_timestamp_column"]
        return values


class SCDJoinParameters(SCDBaseParameters):
    """Parameters for SCD join"""

    left_timestamp_column: InColumnStr


class SCDLookupParameters(SCDBaseParameters):
    """Parameters for SCD lookup"""

    offset: Optional[str]


class LookupNode(AggregationOpStructMixin, BaseNode):
    """LookupNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        input_column_names: List[InColumnStr]
        feature_names: List[OutColumnStr]
        entity_column: InColumnStr
        serving_name: str
        entity_id: PydanticObjectId
        scd_parameters: Optional[SCDLookupParameters]

        @root_validator(skip_on_failure=True)
        @classmethod
        def _validate_input_column_names_feature_names_same_length(
            cls, values: Dict[str, Any]
        ) -> Dict[str, Any]:
            input_column_names = values["input_column_names"]
            feature_names = values["feature_names"]
            assert len(input_column_names) == len(feature_names)
            return values

    type: Literal[NodeType.LOOKUP] = Field(NodeType.LOOKUP, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: Parameters

    def _get_aggregations(
        self,
        columns: List[ViewDataColumn],
        node_name: str,
        other_node_names: Set[str],
        output_var_type: DBVarType,
    ) -> List[AggregationColumn]:
        name_to_column = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=feature_name,
                method=None,
                keys=[self.parameters.entity_column],
                window=None,
                category=None,
                column=name_to_column[input_column_name],
                aggregation_type=self.type,
                node_names={node_name}.union(other_node_names),
                node_name=node_name,
                filter=any(col.filter for col in columns),
                dtype=name_to_column[input_column_name].dtype,
            )
            for input_column_name, feature_name in zip(
                self.parameters.input_column_names, self.parameters.feature_names
            )
        ]

    def _exclude_source_columns(self) -> List[str]:
        return [self.parameters.entity_column]


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
        scd_parameters: Optional[SCDJoinParameters]

        @validator(
            "left_input_columns",
            "right_input_columns",
            "left_output_columns",
            "right_output_columns",
        )
        @classmethod
        def _validate_columns_are_unique(cls, values: List[str]) -> List[str]:
            if len(values) != len(set(values)):
                raise ValueError(f"Column names (values: {values}) must be unique!")
            return values

        @root_validator
        @classmethod
        def _validate_left_and_right_output_columns(cls, values: Dict[str, Any]) -> Dict[str, Any]:
            duplicated_output_cols = set(values.get("left_output_columns", [])).intersection(
                values.get("right_output_columns", [])
            )
            if duplicated_output_cols:
                raise ValueError("Left and right output columns should not have common item(s).")
            return values

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
        # construct input column name to output column name mapping for left & right columns
        left_col_map = dict(zip(params.left_input_columns, params.left_output_columns))
        right_col_map = dict(zip(params.right_input_columns, params.right_output_columns))

        # construct input column name to output column mapping for left & right columns
        left_columns = {
            col.name: col.clone(
                name=left_col_map[col.name],  # type: ignore
                node_names=col.node_names.union([self.name]),
                node_name=self.name,
            )
            for col in inputs[0].columns
            if col.name in left_col_map
        }
        right_columns = {
            col.name: col.clone(
                name=right_col_map[col.name],  # type: ignore
                node_names=col.node_names.union([self.name]),
                node_name=self.name,
            )
            for col in inputs[1].columns
            if col.name in right_col_map
        }

        if self.parameters.join_type == "left":
            row_index_lineage = inputs[0].row_index_lineage
        else:
            row_index_lineage = append_to_lineage(inputs[0].row_index_lineage, self.name)

        # construct left & right output columns
        left_cols = [left_columns[col_name] for col_name in params.left_input_columns]
        right_cols = [right_columns[col_name] for col_name in params.right_input_columns]

        return OperationStructure(
            columns=left_cols + right_cols,
            output_type=NodeOutputType.FRAME,
            output_category=NodeOutputCategory.VIEW,
            row_index_lineage=row_index_lineage,
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
            new_column_var_type=feature_operation_info.series_output_dtype,
        )


class AggregateAsAtParameters(BaseGroupbyParameters, SCDBaseParameters):
    """Parameters for AggregateAsAtNode"""

    name: OutColumnStr
    offset: Optional[str]
    backward: Optional[bool]


class AggregateAsAtNode(AggregationOpStructMixin, BaseNode):
    """AggregateAsAt class"""

    type: Literal[NodeType.AGGREGATE_AS_AT] = Field(NodeType.AGGREGATE_AS_AT, const=True)
    output_type: NodeOutputType = Field(NodeOutputType.FRAME, const=True)
    parameters: AggregateAsAtParameters

    def _exclude_source_columns(self) -> List[str]:
        return [str(key) for key in self.parameters.keys]

    def _get_aggregations(
        self,
        columns: List[ViewDataColumn],
        node_name: str,
        other_node_names: Set[str],
        output_var_type: DBVarType,
    ) -> List[AggregationColumn]:
        col_name_map = {col.name: col for col in columns}
        return [
            AggregationColumn(
                name=self.parameters.name,
                method=self.parameters.agg_func,
                keys=self.parameters.keys,
                window=None,
                category=None,
                column=col_name_map.get(self.parameters.parent),
                filter=any(col.filter for col in columns),
                aggregation_type=self.type,
                node_names={node_name}.union(other_node_names),
                node_name=node_name,
                dtype=output_var_type,
            )
        ]


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
                name=new_name,
                node_names=last_col.node_names.union([self.name]),
                node_name=self.name,
            )
        else:
            last_agg = input_operation_info.aggregations[-1]
            node_kwargs["columns"] = input_operation_info.columns
            node_kwargs["aggregations"] = list(input_operation_info.aggregations)
            node_kwargs["aggregations"][-1] = last_agg.clone(
                name=new_name,
                node_names=last_agg.node_names.union([self.name]),
                node_name=self.name,
            )

        return OperationStructure(
            **node_kwargs,
            output_type=self.output_type,
            output_category=output_category,
            row_index_lineage=input_operation_info.row_index_lineage,
        )
