"""
This module contains an extractor class to generate SDK codes from a query graph.
"""

from typing import Any, Dict, List, Optional, Set, Tuple

from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.config import SDKCodeGenConfig
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationContext,
    CodeGenerator,
    ExpressionStr,
    NodeCodeGenOutput,
    StatementStr,
    StatementT,
    VariableNameGenerator,
    get_object_class_from_function_call,
)
from featurebyte.query_graph.node.schema import DatabaseDetails
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor


class SDKCodeGlobalState(FeatureByteBaseModel):
    """
    SDKCodeGlobalState class

    node_name_to_post_compute_output: Dict[str, VarNameExpressionStr]
        Cache to store node name to _post_compute_output (to remove redundant graph node traversals)
    node_name_to_operation_structure: Dict[str, OperationStructure]
        Operation structure mapping for each node in the graph
    code_generation_config: SDKCodeGenConfig
        Code generation configuration
    var_name_generator: VariableNameGenerator
        Variable name generator is used to generate variable names used in the generated SDK codes
    code_generator: CodeGenerator
        Code generator is used to generate final SDK codes from a list of statements & imports
    """

    node_name_to_post_compute_output: Dict[str, NodeCodeGenOutput] = Field(default_factory=dict)
    node_name_to_operation_structure: Dict[str, OperationStructure] = Field(default_factory=dict)
    code_generation_config: SDKCodeGenConfig = Field(default_factory=SDKCodeGenConfig)
    var_name_generator: VariableNameGenerator = Field(default_factory=VariableNameGenerator)
    code_generator: CodeGenerator = Field(default_factory=CodeGenerator)
    no_op_node_names: Set[str] = Field(default_factory=set)
    as_info_dict_node_names: Set[str] = Field(default_factory=set)
    required_copy_node_names: Set[str] = Field(default_factory=set)

    def _identify_output_as_info_dict(
        self,
        node: Node,
        query_graph: QueryGraphModel,
        node_name_to_operation_structure: Dict[str, OperationStructure],
    ) -> None:
        """
        Identify the node names that should output info dict rather than variable name or expression.

        Parameters
        ----------
        node: Node
            Node to check
        query_graph: QueryGraphModel
            Query graph
        node_name_to_operation_structure: Dict[str, OperationStructure]
            Operation structure mapping for each node in the graph
        """
        forward_nodes = [
            query_graph.nodes_map[name] for name in query_graph.edges_map.get(node.name, [])
        ]
        if (
            node.type == NodeType.CONDITIONAL
            and len(forward_nodes) == 1
            and forward_nodes[0].type == NodeType.ASSIGN
        ):
            # There are 2 cases we need to consider:
            # * `view[<col_name>][<mask>] = <value>`
            # * `col[<mask>] = <value>; view[<col_name>] = col`
            #
            # If `view` has `col_name` column, then `view[<col_name>]` must have parent. In this case, we should
            # use first case to generate SDK code. If we use the second case to generate the graph,
            # `col[<mask>] = <value>` will update the column's parent view, and the `view[<col_name>] = col`
            # will update the column's parent view again. The generated graph will be
            # `<CONDITIONAL> -> <ASSIGN> -> <ASSIGN>` but not `<CONDITIONAL> -> <ASSIGN>`.
            #
            # On the other hand, if `view` doesn't have `<col_name>` column, then `view[<col_name>][<mask>]`
            # will throw an error. In this case, we should use the second case to generate SDK code.
            assign_node = forward_nodes[0]
            assign_node_input_node_names = query_graph.backward_edges_map[assign_node.name]
            assign_input_view_op_struct = node_name_to_operation_structure[
                assign_node_input_node_names[0]
            ]
            assign_input_view_columns = assign_input_view_op_struct.output_column_names
            assign_column_name = assign_node.parameters.name  # type: ignore
            if assign_column_name in assign_input_view_columns:
                # This handle the case `view[<col>][<mask>] = <value>`. Since this SDK code actually contains
                # ConditionalNode and AssignNode, we need to output info dict for the ConditionalNode and
                # postpone the assignment operation to AssignNode.
                self.as_info_dict_node_names.add(node.name)

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
            NodeType.FORWARD_AGGREGATE,
            NodeType.LOOKUP_TARGET,
            NodeType.ITEM_GROUPBY,
            NodeType.AGGREGATE_AS_AT,
            NodeType.FORWARD_AGGREGATE_AS_AT,
        ):
            # ForwardAggregateNode's, LookupTargetNode's, ItemGroupByNode's & AggregateAsAtNode's SDK code like
            # * `view.groupby(...).forward_aggregate(...)`
            # * `view.as_target(...)`
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
        if (
            node.is_inplace_operation_in_sdk_code
            and len(set(edges_map[backward_nodes[0].name])) > 1
        ):
            # If there are more than one distinct output nodes from the first input, it means that the input
            # api object is used by other operations. In this case, we need to copy the input api object to avoid
            # inplace operation affecting other operation. Currently, only api object from the first input node
            # could be affected by the inplace operation.
            #
            # For example:
            #   grouped = view.groupby(...).aggregate(...)
            #   view_1 = view.copy()
            #   view_1[<col>] = <value>
            #
            # In this example, a copy is made for the `view` object because the `view` object is used by
            # GroupByNode and AssignNode and the AssignNode is an inplace operation.
            self.required_copy_node_names.add(node.name)

    def initialize(
        self,
        query_graph: QueryGraphModel,
        node_name_to_operation_structure: Dict[str, OperationStructure],
    ) -> None:
        """
        Initialize SDKCodeGlobalState

        Parameters
        ----------
        query_graph: QueryGraphModel
            Query graph
        node_name_to_operation_structure: Dict[str, OperationStructure]
            Operation structure mapping for each node in the graph
        """
        for node in query_graph.iterate_sorted_nodes():
            backward_nodes = [
                query_graph.nodes_map[name]
                for name in query_graph.backward_edges_map.get(node.name, [])
            ]
            self._identify_output_as_info_dict(node, query_graph, node_name_to_operation_structure)
            self._identify_no_op_node(node, backward_nodes)
            self._identify_required_copy_node(node, backward_nodes, query_graph.edges_map)


class SDKCodeExtractor(
    BaseGraphExtractor[SDKCodeGlobalState, FeatureByteBaseModel, SDKCodeGlobalState]
):
    """
    SDKCodeExtractor class is responsible to generate SDK codes from a given query graph.
    """

    def _pre_compute(
        self,
        branch_state: FeatureByteBaseModel,
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
        branch_state: FeatureByteBaseModel,
        global_state: SDKCodeGlobalState,
        node: Node,
        input_node: Node,
    ) -> FeatureByteBaseModel:
        return branch_state

    def _post_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: SDKCodeGlobalState,
        node: Node,
        inputs: List[NodeCodeGenOutput],
        skip_post: bool,
    ) -> NodeCodeGenOutput:
        if node.name in global_state.node_name_to_post_compute_output:
            return global_state.node_name_to_post_compute_output[node.name]

        # construct SDK code
        op_struct = global_state.node_name_to_operation_structure[node.name]

        statements: List[StatementT]
        if node.name in global_state.no_op_node_names:
            var_name_or_expr = inputs[0].var_name_or_expr
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
                    as_info_dict=node.name in global_state.as_info_dict_node_names,
                    required_copy=node.name in global_state.required_copy_node_names,
                ),
            )

        # update global state
        post_compute_output = NodeCodeGenOutput(
            var_name_or_expr=var_name_or_expr,
            operation_structure=global_state.node_name_to_operation_structure[node.name],
        )
        global_state.code_generator.add_statements(statements=statements)
        global_state.node_name_to_post_compute_output[node.name] = post_compute_output

        # return the output variable name or expression of current operation so that
        # it can be passed as `inputs` to the next node's post compute operation
        return post_compute_output

    def extract(
        self,
        node: Node,
        to_use_saved_data: bool = False,
        feature_store_name: Optional[str] = None,
        feature_store_id: Optional[ObjectId] = None,
        database_details: Optional[DatabaseDetails] = None,
        table_id_to_info: Optional[Dict[ObjectId, Dict[str, Any]]] = None,
        output_id: Optional[ObjectId] = None,
        last_statement_callback: Optional[Any] = None,
        **kwargs: Any,
    ) -> SDKCodeGlobalState:
        op_struct_info = OperationStructureExtractor(graph=self.graph).extract(node=node)

        code_generation_config: Dict[str, Any] = {"to_use_saved_data": to_use_saved_data}
        if feature_store_name:
            code_generation_config["feature_store_name"] = feature_store_name
        if feature_store_id:
            code_generation_config["feature_store_id"] = feature_store_id
        if database_details:
            code_generation_config["database_details"] = database_details
        if table_id_to_info:
            code_generation_config["table_id_to_info"] = table_id_to_info

        global_state = SDKCodeGlobalState(
            node_name_to_operation_structure=op_struct_info.operation_structure_map,
            code_generation_config=SDKCodeGenConfig(**code_generation_config),
        )
        global_state.initialize(
            query_graph=self.graph,
            node_name_to_operation_structure=op_struct_info.operation_structure_map,
        )
        node_codegen_output = self._extract(
            node=node,
            branch_state=FeatureByteBaseModel(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        var_name_or_expr = node_codegen_output.var_name_or_expr
        final_output_name = global_state.code_generation_config.final_output_name
        output_var = global_state.var_name_generator.convert_to_variable_name(
            final_output_name, node_name=None
        )
        if last_statement_callback:
            global_state.code_generator.add_statements(
                statements=last_statement_callback(output_var, var_name_or_expr)
            )
        else:
            global_state.code_generator.add_statements(statements=[(output_var, var_name_or_expr)])

        if output_id:
            statement = get_object_class_from_function_call(
                callable_name=f"{output_var}.save", _id=ClassEnum.OBJECT_ID(output_id)
            )
            global_state.code_generator.add_statements(statements=[StatementStr(statement)])
        return global_state
