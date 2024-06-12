"""
This module contains nested graph related node classes
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from typing_extensions import Annotated, Literal

from abc import ABC, abstractmethod  # pylint: disable=wrong-import-order

from pydantic import Field

from featurebyte.enum import DBVarType, SpecialColumnName, ViewMode
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.enum import (
    FEAST_TIMESTAMP_POSTFIX,
    GraphNodeType,
    NodeOutputType,
    NodeType,
)
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node.base import BaseNode, BasePrunableNode, NodeT
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.metadata.config import (
    OnDemandFunctionCodeGenConfig,
    OnDemandViewCodeGenConfig,
    SDKCodeGenConfig,
)
from featurebyte.query_graph.node.metadata.operation import (
    OperationStructure,
    OperationStructureInfo,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationContext,
    ExpressionStr,
    ObjectClass,
    StatementStr,
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
    get_object_class_from_function_call,
)
from featurebyte.query_graph.node.utils import subset_frame_column_expr
from featurebyte.typing import Scalar


class ProxyInputNode(BaseNode):
    """Proxy input node used by nested graph"""

    class ProxyInputNodeParameters(FeatureByteBaseModel):
        """Proxy input node parameters"""

        input_order: int

    type: Literal[NodeType.PROXY_INPUT] = Field(NodeType.PROXY_INPUT, const=True)
    output_type: NodeOutputType
    parameters: ProxyInputNodeParameters

    @property
    def max_input_count(self) -> int:
        return 0

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        raise RuntimeError("Proxy input node should not be used to derive input columns.")

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        # lookup the operation structure using the proxy input's node_name parameter
        proxy_input_order = self.parameters.input_order
        operation_structure = global_state.proxy_input_operation_structures[proxy_input_order]
        return OperationStructure(
            columns=[
                col.clone(node_names={self.name}, node_name=self.name)
                for col in operation_structure.columns
            ],
            aggregations=[
                agg.clone(node_names={self.name}, node_name=self.name)
                for agg in operation_structure.aggregations
            ],
            output_type=operation_structure.output_type,
            output_category=operation_structure.output_category,
            row_index_lineage=operation_structure.row_index_lineage,
        )


class BaseGraphNodeParameters(FeatureByteBaseModel):
    """Graph node parameters"""

    graph: "QueryGraphModel"  # type: ignore[name-defined]
    output_node_name: str
    type: GraphNodeType

    @abstractmethod
    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        node_name: str,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        """
        Derive SDK code for the current graph node

        Parameters
        ----------
        input_var_name_expressions: List[VarNameExpressionStr]
            Input variables name
        var_name_generator: VariableNameGenerator
            Variable name generator
        operation_structure: OperationStructure
            Operation structure of current node
        config: SDKCodeGenConfig
            Code generation configuration
        node_name: str
            Node name of the current graph node

        Returns
        -------
        Tuple[List[StatementT], VarNameExpressionStr]
        """


class CleaningGraphNodeParameters(BaseGraphNodeParameters):
    """GraphNode (type:cleaning) parameters"""

    type: Literal[GraphNodeType.CLEANING] = Field(GraphNodeType.CLEANING, const=True)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        node_name: str,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        raise RuntimeError("Not implemented")


class AggregationNodeInfo(FeatureByteBaseModel):
    """
    AggregationNodeInfo class stores information about the aggregation-type node.
    """

    node_type: NodeType
    input_node_name: Optional[str]
    node_name: str


class OfflineStoreMetadata(FeatureByteBaseModel):
    """
    Offline store metadata
    """

    aggregation_nodes_info: List[AggregationNodeInfo]
    feature_job_setting: Optional[FeatureJobSetting]
    has_ttl: bool
    offline_store_table_name: str
    output_dtype: DBVarType
    primary_entity_dtypes: List[DBVarType]
    null_filling_value: Optional[Scalar] = Field(default=None)


class OfflineStoreIngestQueryGraphNodeParameters(OfflineStoreMetadata, BaseGraphNodeParameters):
    """
    Class used for offline store ingest query graph node parameters
    """

    type: Literal[GraphNodeType.OFFLINE_STORE_INGEST_QUERY] = Field(
        GraphNodeType.OFFLINE_STORE_INGEST_QUERY, const=True
    )
    output_column_name: str
    primary_entity_ids: List[PydanticObjectId]

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        node_name: str,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        raise RuntimeError("Not implemented")


ViewMetadataT = TypeVar("ViewMetadataT", bound="ViewMetadata")


class ViewMetadata(FeatureByteBaseModel):
    """View metadata (used by event, scd & dimension view)"""

    view_mode: ViewMode
    drop_column_names: List[str]
    column_cleaning_operations: List[ColumnCleaningOperation]
    table_id: PydanticObjectId

    def clone(
        self: ViewMetadataT,
        view_mode: ViewMode,
        column_cleaning_operations: List[ColumnCleaningOperation],
        **kwargs: Any,
    ) -> ViewMetadataT:
        """
        Clone the current instance by replacing column cleaning operations with the
        given column cleaning operations.

        Parameters
        ----------
        view_mode: ViewMode
            View mode
        column_cleaning_operations: List[ColumnCleaningOperation]
            Column cleaning operations
        kwargs: Any
            Additional keyword arguments

        Returns
        -------
        ViewMetadataT
        """
        return type(self)(
            **{
                **self.dict(by_alias=True),
                "view_mode": view_mode,
                "column_cleaning_operations": column_cleaning_operations,
                **kwargs,
            }
        )


class BaseViewGraphNodeParameters(BaseGraphNodeParameters, ABC):
    """BaseViewGraphNodeParameters class"""

    metadata: ViewMetadata

    def prune_metadata(self, target_columns: List[str], input_nodes: Sequence[NodeT]) -> Any:
        """
        Prune metadata for the current graph node

        Parameters
        ----------
        target_columns: List[str]
            Target columns
        input_nodes: Sequence[NodeT]
            Input nodes

        Returns
        -------
        ParameterT
        """
        return type(self.metadata)(**self._prune_metadata(target_columns, input_nodes))

    def _prune_metadata(
        self, target_columns: List[str], input_nodes: Sequence[NodeT]
    ) -> Dict[str, Any]:
        _ = input_nodes
        metadata = self.metadata.dict(by_alias=True)
        metadata["column_cleaning_operations"] = [
            col
            for col in self.metadata.column_cleaning_operations
            if col.column_name in target_columns
        ]
        return metadata

    @staticmethod
    def prepare_column_cleaning_operation_code_generation(
        column_cleaning_operations: List[ColumnCleaningOperation],
    ) -> List[ObjectClass]:
        """
        Prepare column cleaning operation code generation

        Parameters
        ----------
        column_cleaning_operations: List[ColumnCleaningOperation]
            Column cleaning operations to be converted to SDK code

        Returns
        -------
        List[ClassEnum.COLUMN_CLEANING_OPERATION]
        """
        return [
            ClassEnum.COLUMN_CLEANING_OPERATION(
                column_name=col_clean_op.column_name,
                cleaning_operations=[
                    col.derive_sdk_code() for col in col_clean_op.cleaning_operations
                ],
            )
            for col_clean_op in column_cleaning_operations
        ]


class EventViewGraphNodeParameters(BaseViewGraphNodeParameters):
    """GraphNode (type:event_view) parameters"""

    type: Literal[GraphNodeType.EVENT_VIEW] = Field(GraphNodeType.EVENT_VIEW, const=True)

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        node_name: str,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        # construct event view sdk statement
        view_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="event_view", node_name=node_name
        )
        assert len(input_var_name_expressions) == 1
        table_var_name = input_var_name_expressions[0]
        expression = get_object_class_from_function_call(
            callable_name=f"{table_var_name}.get_view",
            view_mode=ViewMode.MANUAL,
            drop_column_names=self.metadata.drop_column_names,
            column_cleaning_operations=self.prepare_column_cleaning_operation_code_generation(
                column_cleaning_operations=self.metadata.column_cleaning_operations
            ),
        )
        return [(view_var_name, expression)], view_var_name


class ItemViewMetadata(ViewMetadata):
    """Item view metadata"""

    event_suffix: Optional[str]
    event_drop_column_names: List[str]
    event_column_cleaning_operations: List[ColumnCleaningOperation]
    event_join_column_names: List[str]
    event_table_id: PydanticObjectId


class ItemViewGraphNodeParameters(BaseViewGraphNodeParameters):
    """GraphNode (type:item_view) parameters"""

    type: Literal[GraphNodeType.ITEM_VIEW] = Field(GraphNodeType.ITEM_VIEW, const=True)
    metadata: ItemViewMetadata

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        node_name: str,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        # construct item view sdk statement
        view_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="item_view", node_name=node_name
        )
        assert len(input_var_name_expressions) == 1
        table_var_name = input_var_name_expressions[0]
        expression = get_object_class_from_function_call(
            callable_name=f"{table_var_name}.get_view",
            event_suffix=self.metadata.event_suffix,
            view_mode=ViewMode.MANUAL,
            drop_column_names=self.metadata.drop_column_names,
            column_cleaning_operations=self.prepare_column_cleaning_operation_code_generation(
                column_cleaning_operations=self.metadata.column_cleaning_operations
            ),
            event_drop_column_names=self.metadata.event_drop_column_names,
            event_column_cleaning_operations=self.prepare_column_cleaning_operation_code_generation(
                column_cleaning_operations=self.metadata.event_column_cleaning_operations
            ),
            event_join_column_names=self.metadata.event_join_column_names,
        )
        return [(view_var_name, expression)], view_var_name

    def _prune_metadata(
        self, target_columns: List[str], input_nodes: Sequence[NodeT]
    ) -> Dict[str, Any]:
        metadata = super()._prune_metadata(target_columns=target_columns, input_nodes=input_nodes)
        # for item view graph node, we need to use the event view graph node's metadata
        # to generate the event column cleaning operations
        assert len(input_nodes) == 2
        event_view_node = input_nodes[1]
        assert isinstance(event_view_node.parameters, EventViewGraphNodeParameters)
        event_view_metadata = event_view_node.parameters.metadata
        metadata["event_column_cleaning_operations"] = (
            event_view_metadata.column_cleaning_operations
        )
        return metadata


class DimensionViewGraphNodeParameters(BaseViewGraphNodeParameters):
    """GraphNode (type:dimension_view) parameters"""

    type: Literal[GraphNodeType.DIMENSION_VIEW] = Field(GraphNodeType.DIMENSION_VIEW, const=True)

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        node_name: str,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        # construct dimension view sdk statement
        view_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="dimension_view", node_name=node_name
        )
        assert len(input_var_name_expressions) == 1
        table_var_name = input_var_name_expressions[0]
        expression = get_object_class_from_function_call(
            f"{table_var_name}.get_view",
            view_mode=ViewMode.MANUAL,
            drop_column_names=self.metadata.drop_column_names,
            column_cleaning_operations=self.prepare_column_cleaning_operation_code_generation(
                column_cleaning_operations=self.metadata.column_cleaning_operations
            ),
        )
        return [(view_var_name, expression)], view_var_name


class SCDViewGraphNodeParameters(BaseViewGraphNodeParameters):
    """GraphNode (type:scd_view) parameters"""

    type: Literal[GraphNodeType.SCD_VIEW] = Field(GraphNodeType.SCD_VIEW, const=True)

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        node_name: str,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        # construct scd view sdk statement
        view_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="scd_view", node_name=node_name
        )
        assert len(input_var_name_expressions) == 1
        table_var_name = input_var_name_expressions[0]
        expression = get_object_class_from_function_call(
            f"{table_var_name}.get_view",
            view_mode=ViewMode.MANUAL,
            drop_column_names=self.metadata.drop_column_names,
            column_cleaning_operations=self.prepare_column_cleaning_operation_code_generation(
                column_cleaning_operations=self.metadata.column_cleaning_operations
            ),
        )
        return [(view_var_name, expression)], view_var_name


class ChangeViewMetadata(ViewMetadata):
    """Change view metadata"""

    track_changes_column: str
    default_feature_job_setting: Optional[Dict[str, Any]]
    prefixes: Optional[Tuple[Optional[str], Optional[str]]]


class ChangeViewGraphNodeParameters(BaseViewGraphNodeParameters):
    """GraphNode (type:change_view) parameters"""

    type: Literal[GraphNodeType.CHANGE_VIEW] = Field(GraphNodeType.CHANGE_VIEW, const=True)
    metadata: ChangeViewMetadata

    def derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        node_name: str,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        # construct change view sdk statement
        view_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="change_view", node_name=node_name
        )

        feature_job_setting: Optional[ObjectClass] = None
        if self.metadata.default_feature_job_setting:
            feature_job_setting = ClassEnum.FEATURE_JOB_SETTING(
                **self.metadata.default_feature_job_setting
            )

        assert len(input_var_name_expressions) == 1
        table_var_name = input_var_name_expressions[0]
        expression = get_object_class_from_function_call(
            f"{table_var_name}.get_change_view",
            track_changes_column=self.metadata.track_changes_column,
            default_feature_job_setting=feature_job_setting,
            prefixes=self.metadata.prefixes,
            view_mode=ViewMode.MANUAL,
            drop_column_names=self.metadata.drop_column_names,
            column_cleaning_operations=self.prepare_column_cleaning_operation_code_generation(
                column_cleaning_operations=self.metadata.column_cleaning_operations
            ),
        )
        return [(view_var_name, expression)], view_var_name


GRAPH_NODE_PARAMETERS_TYPES = [
    CleaningGraphNodeParameters,
    OfflineStoreIngestQueryGraphNodeParameters,
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


class BaseGraphNode(BasePrunableNode):
    """Graph node"""

    type: Literal[NodeType.GRAPH] = Field(NodeType.GRAPH, const=True)
    output_type: NodeOutputType
    parameters: GraphNodeParameters

    @property
    def transform_info(self) -> str:
        return ""

    @property
    def output_node(self) -> NodeT:  # type: ignore[type-var]
        """
        Output node of the graph (in the graph node)

        Returns
        -------
        NodeT
        """
        return cast(NodeT, self.parameters.graph.nodes_map[self.parameters.output_node_name])

    @property
    def max_input_count(self) -> int:
        node_iter = self.parameters.graph.iterate_nodes(
            target_node=self.output_node, node_type=NodeType.PROXY_INPUT
        )
        return len(list(node_iter))

    @property
    def is_prunable(self) -> bool:
        """
        Whether the graph node is prunable

        Returns
        -------
        bool
        """
        return self.parameters.type == GraphNodeType.CLEANING

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        # first the corresponding input proxy node in the nested graph
        proxy_input_node: Optional[BaseNode] = None
        for node in self.parameters.graph.iterate_nodes(
            target_node=self.output_node, node_type=NodeType.PROXY_INPUT
        ):
            assert isinstance(node, ProxyInputNode)
            if node.parameters.input_order == input_index:
                proxy_input_node = node

        assert proxy_input_node is not None, "Cannot find corresponding proxy input node!"

        # from the proxy input node, find all the target nodes (nodes that use the proxy input node as input)
        # for each target node, find the input order of the proxy input node
        # use the input order to get the required input columns and combine them
        required_input_columns = set()
        target_node_names = self.parameters.graph.edges_map[proxy_input_node.name]
        for target_node_name in target_node_names:
            target_node = self.parameters.graph.nodes_map[target_node_name]
            target_input_node_names = self.parameters.graph.get_input_node_names(target_node)
            input_index = target_input_node_names.index(proxy_input_node.name)
            required_input_columns.update(
                target_node.get_required_input_columns(
                    input_index=input_index, available_column_names=available_column_names
                )
            )
        return list(required_input_columns)

    def resolve_node_pruned(self, input_node_names: List[str]) -> str:
        if self.parameters.type == GraphNodeType.CLEANING:
            return input_node_names[0]

        # other graph node types should not reach here as they are not prunable
        raise RuntimeError("BaseGraphNode.resolve_node_pruned should not be called!")

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        # this should not be called as it should be handled at operation structure extractor level
        raise RuntimeError("BaseGroupNode._derive_node_operation_info should not be called!")

    def prune(
        self: NodeT,
        target_node_input_order_pairs: Sequence[Tuple[NodeT, int]],
        input_operation_structures: List[OperationStructure],
    ) -> NodeT:
        raise RuntimeError("BaseGroupNode.prune should not be called!")

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        return self.parameters.derive_sdk_code(
            input_var_name_expressions=node_inputs,
            var_name_generator=var_name_generator,
            operation_structure=operation_structure,
            config=config,
            node_name=self.name,
        )

    def _derive_on_demand_view_or_user_defined_function_helper(
        self,
        var_name_generator: VariableNameGenerator,
        input_var_name_expr: VariableNameStr,
        json_conversion_func: Callable[[VarNameExpressionInfo], ExpressionStr],
        null_filling_func: Callable[[VarNameExpressionInfo, ValueStr], ExpressionStr],
        is_databricks_udf: bool,
        config_for_ttl: Optional[OnDemandViewCodeGenConfig] = None,
        ttl_handling_column: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        if self.parameters.type != GraphNodeType.OFFLINE_STORE_INGEST_QUERY:
            raise RuntimeError("BaseGroupNode._derive_on_demand_view_code should not be called!")

        statements: List[StatementT] = []
        node_params = self.parameters
        assert isinstance(node_params, OfflineStoreIngestQueryGraphNodeParameters)
        if node_params.null_filling_value is not None:
            var = var_name_generator.convert_to_variable_name("feat", node_name=self.name)
            null_fill_expr = null_filling_func(
                input_var_name_expr, ValueStr(node_params.null_filling_value)
            )
            statements.append((var, null_fill_expr))
            input_var_name_expr = var

        if ttl_handling_column is not None:
            assert config_for_ttl is not None
            assert ttl_seconds is not None
            input_df_name = config_for_ttl.input_df_name
            feat_ts_col = f"{ttl_handling_column}{FEAST_TIMESTAMP_POSTFIX}"
            # need to apply TTL handling on the input column
            var_name_map = {}
            for var_name in ["request_time", "cutoff", "feat_ts", "mask"]:
                var_name_map[var_name] = var_name_generator.convert_to_variable_name(
                    variable_name_prefix=var_name, node_name=None
                )

            # request_time = pd.to_datetime(input_df_name["POINT_IN_TIME"])
            statements.append(
                (
                    var_name_map["request_time"],
                    self._to_datetime_expr(
                        ExpressionStr(
                            subset_frame_column_expr(
                                VariableNameStr(input_df_name),
                                SpecialColumnName.POINT_IN_TIME.value,
                            )
                        ),
                        to_handle_none=is_databricks_udf,
                    ),
                )
            )
            statements.append(  # cutoff = request_time - pd.Timedelta(seconds=ttl_seconds)
                (
                    var_name_map["cutoff"],
                    ExpressionStr(
                        f"{var_name_map['request_time']} - pd.Timedelta(seconds={ttl_seconds})"
                    ),
                )
            )
            statements.append(  # feature_ts = pd.to_datetime(input_df_name[ttl_handling_column], unit="s", utc=True)
                (
                    var_name_map["feat_ts"],
                    self._to_datetime_expr(
                        ExpressionStr(
                            subset_frame_column_expr(VariableNameStr(input_df_name), feat_ts_col)
                        ),
                        to_handle_none=False,
                        unit="s",
                    ),
                )
            )
            statements.append(  # mask = (feature_ts >= cutoff) & (feature_ts <= request_time)
                (
                    var_name_map["mask"],
                    ExpressionStr(
                        f"({var_name_map['feat_ts']} >= {var_name_map['cutoff']}) & "
                        f"({var_name_map['feat_ts']} <= {var_name_map['request_time']})"
                    ),
                )
            )
            statements.append(  # inputs.loc["feat"][~mask] = np.nan
                StatementStr(
                    f"{input_df_name}.loc[~{var_name_map['mask']}, {repr(ttl_handling_column)}] = np.nan"
                )
            )

        if node_params.output_dtype in DBVarType.supported_timestamp_types():
            var_name = var_name_generator.convert_to_variable_name("feat", node_name=self.name)
            statements.append(
                (
                    var_name,
                    self._to_datetime_expr(input_var_name_expr, to_handle_none=is_databricks_udf),
                )
            )
            return statements, var_name

        if node_params.output_dtype in DBVarType.json_conversion_types():
            var_name = var_name_generator.convert_to_variable_name("feat", node_name=self.name)
            json_conv_expr = json_conversion_func(input_var_name_expr)
            statements.append((var_name, json_conv_expr))
            return statements, var_name
        return statements, input_var_name_expr

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        def _json_conversion_func(expr: VarNameExpressionInfo) -> ExpressionStr:
            out_expr = get_object_class_from_function_call(
                f"{expr}.apply",
                ExpressionStr("lambda x: np.nan if pd.isna(x) else json.loads(x)"),
            )
            return ExpressionStr(out_expr)

        input_df_name = config.input_df_name
        column_name = cast(
            OfflineStoreIngestQueryGraphNodeParameters, self.parameters
        ).output_column_name
        input_var_name_expr = VariableNameStr(
            subset_frame_column_expr(frame_name=input_df_name, column_name=column_name)
        )
        ttl_handling_column = None
        config_for_ttl = None
        ttl_seconds = None
        assert isinstance(self.parameters, OfflineStoreIngestQueryGraphNodeParameters)
        if self.parameters.has_ttl:
            ttl_handling_column = column_name
            config_for_ttl = config
            assert self.parameters.feature_job_setting is not None
            ttl_seconds = 2 * self.parameters.feature_job_setting.period_seconds

        return self._derive_on_demand_view_or_user_defined_function_helper(
            var_name_generator=var_name_generator,
            input_var_name_expr=input_var_name_expr,
            json_conversion_func=_json_conversion_func,
            null_filling_func=lambda expr, val: ExpressionStr(f"{expr}.fillna({val.as_input()})"),
            config_for_ttl=config_for_ttl,
            ttl_handling_column=ttl_handling_column,
            ttl_seconds=ttl_seconds,
            is_databricks_udf=False,
        )

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        output_dtype = cast(
            OfflineStoreIngestQueryGraphNodeParameters, self.parameters
        ).output_dtype
        associated_node_name = None
        if (
            output_dtype not in DBVarType.supported_timestamp_types()
            and output_dtype not in DBVarType.json_conversion_types()
        ):
            # if the condition satisfies, it means the following input_var_name is the output of the node
            associated_node_name = self.name

        input_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix=config.input_var_prefix, node_name=associated_node_name
        )
        return self._derive_on_demand_view_or_user_defined_function_helper(
            var_name_generator=var_name_generator,
            input_var_name_expr=input_var_name,
            json_conversion_func=lambda expr: ExpressionStr(
                f"np.nan if pd.isna({expr}) else {expr}"
            ),
            null_filling_func=lambda expr, val: ExpressionStr(
                f"{val.as_input()} if pd.isna({expr}) else {expr}"
            ),
            is_databricks_udf=True,
        )
