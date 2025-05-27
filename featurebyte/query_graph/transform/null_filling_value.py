"""
This graph extractor is responsible for extracting null filling values from the graph.
"""

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from pydantic import Field

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.graph import NodeNameMap, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.mixin import AggregationOpStructMixin
from featurebyte.query_graph.node.request import RequestColumnNode
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.on_demand_function import OnDemandFeatureFunctionExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.typing import Scalar


class NullFillingValueGlobalState(FeatureByteBaseModel):
    """
    NullFillingValueGlobalState encapsulates the global state for null filling value extraction.
    """

    aggregation_node_names: List[str] = Field(default_factory=list)
    graph: QueryGraphModel = Field(default_factory=QueryGraphModel)
    node_name_map: NodeNameMap = Field(default_factory=dict)
    fill_value: Optional[Scalar] = Field(default=None)
    codes: str = Field(default="")
    operation_structure_map: Dict[str, OperationStructure]


class NullFillingValueExtractor(
    BaseGraphExtractor[
        NullFillingValueGlobalState, FeatureByteBaseModel, NullFillingValueGlobalState
    ]
):
    """
    NullFillingValueExtractor is responsible for extracting null filling values from the graph.
    """

    def _pre_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: NullFillingValueGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        input_nodes = [self.graph.get_node_by_name(name) for name in input_node_names]
        if node.type == NodeType.PROJECT and any(
            isinstance(input_node, AggregationOpStructMixin) for input_node in input_nodes
        ):
            # If the node is a project and any of the input nodes are aggregation nodes,
            # then we do not traverse the input nodes and add the node to the aggregation node names.
            global_state.aggregation_node_names.append(node.name)
            return [], False
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: NullFillingValueGlobalState,
        node: Node,
        input_node: Node,
    ) -> FeatureByteBaseModel:
        return branch_state

    def _post_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: NullFillingValueGlobalState,
        node: Node,
        inputs: List[Any],
        skip_post: bool,
    ) -> NullFillingValueGlobalState:
        if node.name in global_state.node_name_map:
            # If the node is already visited, then we skip the post computation.
            return global_state

        # reconstruction of the graph
        mapped_input_nodes = []
        if node.name not in global_state.aggregation_node_names:
            for input_node_name in self.graph.get_input_node_names(node):
                mapped_input_node_name = global_state.node_name_map[input_node_name]
                mapped_input_nodes.append(
                    global_state.graph.get_node_by_name(mapped_input_node_name)
                )

        node_to_insert = node
        if node.name in global_state.aggregation_node_names:
            # for aggregation node, we replace it with a request column node
            op_struct = global_state.operation_structure_map[node.name]
            node_to_insert = RequestColumnNode(
                name="",
                parameters=RequestColumnNode.RequestColumnNodeParameters(
                    column_name="agg_col",
                    dtype_info=op_struct.aggregations[0].dtype_info,
                ),
                output_type=NodeOutputType.SERIES,
            )

        inserted_node = global_state.graph.add_operation_node(
            node=node_to_insert, input_nodes=mapped_input_nodes
        )

        # update the node name map
        global_state.node_name_map[node.name] = inserted_node.name
        return global_state

    def extract(self, node: Node, **kwargs: Any) -> NullFillingValueGlobalState:
        # extract operation structure of the graph that used to identify the dtype of the aggregation
        op_struct_info = OperationStructureExtractor(graph=self.graph).extract(node=node)

        # construct query graph for constructing null filling value extraction function
        state: NullFillingValueGlobalState = self._extract(
            node=node,
            branch_state=FeatureByteBaseModel(),
            global_state=NullFillingValueGlobalState(
                operation_structure_map=op_struct_info.operation_structure_map
            ),
            topological_order_map=self.graph.node_topological_order_map,
        )

        # convert query graph to actual python code
        mapped_node = state.graph.get_node_by_name(state.node_name_map[node.name])
        code_state = OnDemandFeatureFunctionExtractor(graph=state.graph).extract(
            node=mapped_node,
            sql_function_name="",
            sql_input_var_prefix="",
            sql_request_input_var_prefix="",
            sql_comment="",
            function_name="extract_null_filling_value",
            input_var_prefix="",
            request_input_var_prefix="agg_col",
            output_dtype=DBVarType.FLOAT,
            to_generate_null_filling_function=True,
            **kwargs,
        )
        input_values = []
        for sql_input_info in code_state.sql_inputs_info:
            val = "np.nan"
            if sql_input_info.py_type == "pd.Timestamp":
                val = "pd.NaT"
            input_values.append(f"{sql_input_info.py_input_var_name}={val}")

        null_filling_value_codes = code_state.generate_code(to_sql=False)

        # inject null value to the function to extract null filling value
        input_params = ", ".join(input_values)
        null_filling_value_codes += f"\n\nfill_value = extract_null_filling_value({input_params})\n"
        state.codes = null_filling_value_codes
        scope: Dict[str, Any] = {}
        exec(null_filling_value_codes, scope)  # nosec
        fill_value = scope["fill_value"]

        # if fill_value is not null, that implies the query graph has null filling operation
        # that convert the output null value to some other value
        if not pd.isna(fill_value):
            state.fill_value = fill_value
        return state
