"""
This module contains an extractor class to generate SDK codes from a query graph.
"""
from typing import Any, Dict, List, Optional, Set, Tuple

from bson import ObjectId
from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerationConfig,
    CodeGenerationContext,
    CodeGenerator,
    ExpressionStr,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionInfoStr,
)
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor


class SDKCodeGlobalState(BaseModel):
    """
    SDKCodeGlobalState class

    node_name_to_post_compute_output: Dict[str, VarNameExpressionStr]
        Cache to store node name to _post_compute_output (to remove redundant graph node traversals)
    node_name_to_operation_structure: Dict[str, OperationStructure]
        Operation structure mapping for each node in the graph
    code_generation_config: CodeGenerationConfig
        Code generation configuration
    var_name_generator: VariableNameGenerator
        Variable name generator is used to generate variable names used in the generated SDK codes
    code_generator: CodeGenerator
        Code generator is used to generate final SDK codes from a list of statements & imports
    """

    node_name_to_post_compute_output: Dict[str, VarNameExpressionInfoStr] = Field(
        default_factory=dict
    )
    node_name_to_operation_structure: Dict[str, OperationStructure] = Field(default_factory=dict)
    code_generation_config: CodeGenerationConfig = Field(default_factory=CodeGenerationConfig)
    var_name_generator: VariableNameGenerator = Field(default_factory=VariableNameGenerator)
    code_generator: CodeGenerator = Field(default_factory=CodeGenerator)
    no_op_node_names: Set[str] = Field(default_factory=set)
    as_info_str_node_names: Set[str] = Field(default_factory=set)
    required_copy_node_names: Set[str] = Field(default_factory=set)

    def _identify_output_as_info_str(self, node: Node, forward_nodes: List[Node]) -> None:
        """
        Identify the node names that should output info string rather than variable name or expression.

        Parameters
        ----------
        node: Node
            Node to check
        forward_nodes: List[Node]
            Forward nodes of the node
        """
        if (
            node.type == NodeType.CONDITIONAL
            and len(forward_nodes) == 1
            and forward_nodes[0].type == NodeType.ASSIGN
        ):
            # This handle the case `view[<col>][<mask>] = <value>`. Since this SDK code actually contains
            # ConditionalNode and AssignNode, we need to output info string for the ConditionalNode and
            # postpone the assignment operation to AssignNode.
            self.as_info_str_node_names.add(node.name)

    def _identify_no_op_node(self, node: Node, backward_nodes: List[Node]) -> None:
        """
        Identify a node as no-op node by looking at the structure of the node type & its input node types.

        Parameters
        ----------
        node: Node
            Node to check
        backward_nodes: List[Node]
            Backward nodes of the node
        """
        if node.type == NodeType.PROJECT and backward_nodes[0].type in (
            NodeType.ITEM_GROUPBY,
            NodeType.AGGREGATE_AS_AT,
        ):
            # ItemGroupByNode's & AggregateAsAtNode's SDK code like
            # * `view.groupby(...).aggregate(...)`
            # * `view.groupby(...).aggregate_as_at(...)`
            # already return a series, there is no operation for project node to generate any SDK code.
            self.no_op_node_names.add(node.name)

    def _identify_required_copy_node(
        self, node: Node, backward_nodes: List[Node], edges_map: Dict[str, List[str]]
    ) -> None:
        """
        Identify whether a copy is required for the inplace operation node.

        Parameters
        ----------
        node: Node
            Node to check
        backward_nodes: List[Node]
            Backward nodes of the node
        edges_map: Dict[str, str]
            Edges map of the query graph
        """
        if node.type in NodeType.inplace_operation_node_type():
            # get the first input node's output node names
            first_input_node_output_node_names = edges_map[backward_nodes[0].name]
            # get the index of the current node in the first input node's output node names
            cur_node_index = first_input_node_output_node_names.index(node.name)
            # check if there are other nodes after the current node in the first input node's output node names
            if len(set(first_input_node_output_node_names[cur_node_index:])) > 1:
                # If there are other nodes after the current node in the first input node's output node names,
                # it means that the backward node is used by other nodes. In this case, we need to copy the
                # backward node to avoid inplace operation affecting other nodes. Currently, only first node in
                # the backward node's forward nodes will be affected by the inplace operation.
                self.required_copy_node_names.add(node.name)

    def initialize(self, query_graph: QueryGraphModel) -> None:
        """
        Initialize SDKCodeGlobalState

        Parameters
        ----------
        query_graph: QueryGraphModel
            Query graph
        """
        for node in query_graph.iterate_sorted_nodes():
            forward_nodes = [
                query_graph.nodes_map[name] for name in query_graph.edges_map[node.name]
            ]
            backward_nodes = [
                query_graph.nodes_map[name] for name in query_graph.backward_edges_map[node.name]
            ]
            self._identify_output_as_info_str(node, forward_nodes)
            self._identify_no_op_node(node, backward_nodes)
            self._identify_required_copy_node(node, backward_nodes, query_graph.edges_map)


class SDKCodeExtractor(BaseGraphExtractor[SDKCodeGlobalState, BaseModel, SDKCodeGlobalState]):
    """
    SDKCodeExtractor class is responsible to generate SDK codes from a given query graph.
    """

    def _pre_compute(
        self,
        branch_state: BaseModel,
        global_state: SDKCodeGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        if node.type == NodeType.GRAPH and node.parameters.type == GraphNodeType.ITEM_VIEW:  # type: ignore
            # item view consume 2 inputs (item table & event view), when constructing SDK code, we only need to use the
            # first input (item table).
            return input_node_names[:1], False
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: BaseModel,
        global_state: SDKCodeGlobalState,
        node: Node,
        input_node: Node,
    ) -> BaseModel:
        return branch_state

    def _post_compute(
        self,
        branch_state: BaseModel,
        global_state: SDKCodeGlobalState,
        node: Node,
        inputs: List[VarNameExpressionInfoStr],
        skip_post: bool,
    ) -> VarNameExpressionInfoStr:
        if node.name in global_state.node_name_to_post_compute_output:
            return global_state.node_name_to_post_compute_output[node.name]

        # construct SDK code
        op_struct = global_state.node_name_to_operation_structure[node.name]

        statements: List[StatementT]
        if node.name in global_state.no_op_node_names:
            var_name_or_expr = inputs[0]
            statements = []
            if isinstance(var_name_or_expr, ExpressionStr):
                new_var_name = global_state.var_name_generator.generate_variable_name(
                    node_output_type=op_struct.output_type,
                    node_output_category=op_struct.output_category,
                    node_name=node.name,
                )
                statements.append((new_var_name, var_name_or_expr))
                var_name_or_expr = new_var_name
        else:
            statements, var_name_or_expr = node.derive_sdk_code(
                node_inputs=inputs,
                var_name_generator=global_state.var_name_generator,
                operation_structure=op_struct,
                config=global_state.code_generation_config,
                context=CodeGenerationContext(
                    as_info_str=node.name in global_state.as_info_str_node_names,
                    required_copy=node.name in global_state.required_copy_node_names,
                ),
            )

        # update global state
        global_state.code_generator.add_statements(statements=statements)
        global_state.node_name_to_post_compute_output[node.name] = var_name_or_expr

        # return the output variable name or expression of current operation so that
        # it can be passed as `inputs` to the next node's post compute operation
        return var_name_or_expr

    def extract(
        self,
        node: Node,
        to_use_saved_data: bool = False,
        feature_store_name: Optional[str] = None,
        feature_store_id: Optional[ObjectId] = None,
        table_id_to_info: Optional[Dict[ObjectId, Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> SDKCodeGlobalState:
        op_struct_info = OperationStructureExtractor(graph=self.graph).extract(node=node)

        code_generation_config: Dict[str, Any] = {"to_use_saved_data": to_use_saved_data}
        if feature_store_name:
            code_generation_config["feature_store_name"] = feature_store_name
        if feature_store_id:
            code_generation_config["feature_store_id"] = feature_store_id
        if table_id_to_info:
            code_generation_config["table_id_to_info"] = table_id_to_info

        global_state = SDKCodeGlobalState(
            node_name_to_operation_structure=op_struct_info.operation_structure_map,
            code_generation_config=CodeGenerationConfig(**code_generation_config),
        )
        global_state.initialize(query_graph=self.graph)
        var_name_or_expr = self._extract(
            node=node,
            branch_state=BaseModel(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        final_output_name = global_state.code_generation_config.final_output_name
        output_var = global_state.var_name_generator.convert_to_variable_name(
            final_output_name, node_name=None
        )
        global_state.code_generator.add_statements(statements=[(output_var, var_name_or_expr)])
        return global_state
