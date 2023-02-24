"""
This module contains nested graph related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Sequence, Tuple, Union, cast
from typing_extensions import Annotated

from abc import abstractmethod  # pylint: disable=wrong-import-order

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType, ViewMode
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode, NodeT
from featurebyte.query_graph.node.metadata.operation import (
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationConfig,
    ObjectClass,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionStr,
)


class ProxyInputNode(BaseNode):
    """Proxy input node used by nested graph"""

    class ProxyInputNodeParameters(BaseModel):
        """Proxy input node parameters"""

        input_order: int

    type: Literal[NodeType.PROXY_INPUT] = Field(NodeType.PROXY_INPUT, const=True)
    output_type: NodeOutputType
    parameters: ProxyInputNodeParameters

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        # lookup the operation structure using the proxy input's node_name parameter
        proxy_input_order = self.parameters.input_order
        operation_structure = global_state.proxy_input_operation_structures[proxy_input_order]
        return OperationStructure(
            columns=[
                col.clone(node_names=[self.name], node_name=self.name)
                for col in operation_structure.columns
            ],
            aggregations=[
                agg.clone(node_names=[self.name], node_name=self.name)
                for agg in operation_structure.aggregations
            ],
            output_type=operation_structure.output_type,
            output_category=operation_structure.output_category,
            row_index_lineage=operation_structure.row_index_lineage,
        )


class BaseGraphNodeParameters(BaseModel):
    """Graph node parameters"""

    graph: "QueryGraphModel"  # type: ignore[name-defined]
    output_node_name: str
    type: GraphNodeType

    @abstractmethod
    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        """
        Derive SDK code for the current graph node

        Parameters
        ----------
        input_var_name_expressions: List[VarNameExpressionStr]
            Input variables name
        input_node_types: List[NodeType]
            Input node types
        var_name_generator: VariableNameGenerator
            Variable name generator
        operation_structure: OperationStructure
            Operation structure of current node
        config: CodeGenerationConfig
            Code generation configuration

        Returns
        -------
        Tuple[List[StatementT], VarNameExpressionStr]
        """


class BaseCleaningOperation(FeatureByteBaseModel):
    """BaseCleaningOperation class"""

    @abstractmethod
    def add_cleaning_operation(
        self, graph_node: "BaseGraphNode", input_node: NodeT, dtype: DBVarType
    ) -> NodeT:
        """
        Add cleaning operation to the graph node

        Parameters
        ----------
        graph_node: BaseGraphNode
            Nested graph node
        input_node: NodeT
            Input node to the query graph
        dtype: DBVarType
            Data type that output column will be casted to

        Returns
        -------
        Node
        """


class ColumnCleaningOperation(FeatureByteBaseModel):
    """
    ColumnCleaningOperation schema
    """

    column_name: str
    cleaning_operations: Sequence[BaseCleaningOperation]


class DataCleaningOperation(FeatureByteBaseModel):
    """
    DataCleaningOperation schema
    """

    data_name: str
    column_cleaning_operations: List[ColumnCleaningOperation]


class CleaningGraphNodeParameters(BaseGraphNodeParameters):
    """GraphNode (type:cleaning) parameters"""

    type: Literal[GraphNodeType.CLEANING] = Field(GraphNodeType.CLEANING, const=True)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        raise RuntimeError("Not implemented")


class ViewMetadata(BaseModel):
    """View metadata (used by event, scd & dimension view)"""

    view_mode: ViewMode
    drop_column_names: List[str]
    column_cleaning_operations: List[ColumnCleaningOperation]
    data_id: PydanticObjectId


class EventViewGraphNodeParameters(BaseGraphNodeParameters):
    """GraphNode (type:event_view) parameters"""

    type: Literal[GraphNodeType.EVENT_VIEW] = Field(GraphNodeType.EVENT_VIEW, const=True)
    metadata: ViewMetadata

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        # construct event view sdk statement
        view_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="event_view"
        )
        expression = ClassEnum.EVENT_VIEW(
            _method_name="from_event_data", event_data=input_var_name_expressions[0]
        )
        return [(view_var_name, expression)], view_var_name


class ItemViewMetadata(ViewMetadata):
    """Item view metadata"""

    event_suffix: Optional[str]
    event_drop_column_names: List[str]
    event_column_cleaning_operations: List[ColumnCleaningOperation]
    event_join_column_names: List[str]
    event_data_id: PydanticObjectId


class ItemViewGraphNodeParameters(BaseGraphNodeParameters):
    """GraphNode (type:item_view) parameters"""

    type: Literal[GraphNodeType.ITEM_VIEW] = Field(GraphNodeType.ITEM_VIEW, const=True)
    metadata: ItemViewMetadata

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        # construct item view sdk statement
        view_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="item_view"
        )
        expression = ClassEnum.ITEM_VIEW(
            _method_name="from_item_data",
            item_data=input_var_name_expressions[0],
            event_suffix=self.metadata.event_suffix,
        )
        return [(view_var_name, expression)], view_var_name


class DimensionViewGraphNodeParameters(BaseGraphNodeParameters):
    """GraphNode (type:dimension_view) parameters"""

    type: Literal[GraphNodeType.DIMENSION_VIEW] = Field(GraphNodeType.DIMENSION_VIEW, const=True)
    metadata: ViewMetadata

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        # construct dimension view sdk statement
        view_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="dimension_view"
        )
        expression = ClassEnum.DIMENSION_VIEW(
            _method_name="from_dimension_data", dimension_data=input_var_name_expressions[0]
        )
        return [(view_var_name, expression)], view_var_name


class SCDViewGraphNodeParameters(BaseGraphNodeParameters):
    """GraphNode (type:scd_view) parameters"""

    type: Literal[GraphNodeType.SCD_VIEW] = Field(GraphNodeType.SCD_VIEW, const=True)
    metadata: ViewMetadata

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        # construct scd view sdk statement
        view_var_name = var_name_generator.convert_to_variable_name(variable_name_prefix="scd_view")
        expression = ClassEnum.SCD_VIEW(
            _method_name="from_slowly_changing_data",
            slowly_changing_data=input_var_name_expressions[0],
        )
        return [(view_var_name, expression)], view_var_name


class ChangeViewMetadata(ViewMetadata):
    """Change view metadata"""

    track_changes_column: str
    default_feature_job_setting: Optional[Dict[str, Any]]
    prefixes: Optional[Tuple[Optional[str], Optional[str]]]


class ChangeViewGraphNodeParameters(BaseGraphNodeParameters):
    """GraphNode (type:change_view) parameters"""

    type: Literal[GraphNodeType.CHANGE_VIEW] = Field(GraphNodeType.CHANGE_VIEW, const=True)
    metadata: ChangeViewMetadata

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        # construct change view sdk statement
        view_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="change_view"
        )

        feature_job_setting: Optional[ObjectClass] = None
        if self.metadata.default_feature_job_setting:
            feature_job_setting = ClassEnum.FEATURE_JOB_SETTING(
                **self.metadata.default_feature_job_setting
            )

        expression = ClassEnum.CHANGE_VIEW(
            _method_name="from_slowly_changing_data",
            scd_data=input_var_name_expressions[0],
            track_changes_column=self.metadata.track_changes_column,
            default_feature_job_setting=feature_job_setting,
            prefixes=self.metadata.prefixes,
        )
        return [(view_var_name, expression)], view_var_name


GRAPH_NODE_PARAMETERS_TYPES = [
    CleaningGraphNodeParameters,
    EventViewGraphNodeParameters,
    ItemViewGraphNodeParameters,
    DimensionViewGraphNodeParameters,
    SCDViewGraphNodeParameters,
    ChangeViewGraphNodeParameters,
]
if TYPE_CHECKING:
    GraphNodeParameters = BaseGraphNodeParameters
else:
    GraphNodeParameters = Annotated[
        Union[tuple(GRAPH_NODE_PARAMETERS_TYPES)], Field(discriminator="type")
    ]


class BaseGraphNode(BaseNode):
    """Graph node"""

    type: Literal[NodeType.GRAPH] = Field(NodeType.GRAPH, const=True)
    output_type: NodeOutputType
    parameters: GraphNodeParameters

    @property
    def transform_info(self) -> str:
        return self.type

    @property
    def output_node(self) -> NodeT:
        """
        Output node of the graph (in the graph node)

        Returns
        -------
        NodeT
        """
        return cast(NodeT, self.parameters.graph.nodes_map[self.parameters.output_node_name])

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        # this should not be called as it should be handled at operation structure extractor level
        raise RuntimeError("BaseGroupNode._derive_node_operation_info should not be called!")

    def prune(
        self: NodeT,
        target_nodes: Sequence[NodeT],
        input_operation_structures: List[OperationStructure],
    ) -> NodeT:
        raise RuntimeError("BaseGroupNode.prune should not be called!")

    def _derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        return self.parameters.derive_sdk_code(
            input_var_name_expressions=input_var_name_expressions,
            input_node_types=input_node_types,
            var_name_generator=var_name_generator,
            operation_structure=operation_structure,
            config=config,
        )
