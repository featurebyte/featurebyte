"""
OfflineStoreIngestQuery object stores the offline store ingest query for a feature.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from pydantic import validator

from featurebyte.common.validator import construct_sort_validator
from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.mixin import QueryGraphMixin
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.sdk_code import CodeGenerator, VariableNameGenerator
from featurebyte.query_graph.node.nested import (
    AggregationNodeInfo,
    OfflineStoreIngestQueryGraphNodeParameters,
    OfflineStoreMetadata,
)
from featurebyte.query_graph.node.utils import subset_frame_column_expr
from featurebyte.query_graph.transform.on_demand_function import (
    OnDemandFeatureFunctionExtractor,
    OnDemandFeatureFunctionGlobalState,
)
from featurebyte.query_graph.transform.on_demand_view import OnDemandFeatureViewExtractor
from featurebyte.query_graph.transform.quick_pruning import QuickGraphStructurePruningTransformer


class OfflineStoreInfoMetadata(OfflineStoreMetadata):
    """
    OfflineStoreInfoMetadata object stores the offline store table metadata of the feature or target.
    """

    output_column_name: str
    primary_entity_ids: List[PydanticObjectId]


class OfflineStoreIngestQueryGraph(FeatureByteBaseModel):
    """
    OfflineStoreIngestQuery object stores the offline store ingest query for a feature or target.
    """

    # offline ingest graph related info
    # offline store ingest query graph & output node name (from the graph)
    # reference node name that is used in decomposed query graph (if None, the graph is not decomposed)
    # aggregation nodes info of the offline store ingest query graph
    graph: QueryGraphModel
    node_name: str
    ref_node_name: Optional[str]
    aggregation_nodes_info: List[AggregationNodeInfo]

    # table related info
    offline_store_table_name: str
    output_column_name: str
    output_dtype: DBVarType

    # offline store table related metadata
    # primary entity ids of the offline store ingest query graph
    # feature job setting of the offline store ingest query graph
    # whether the offline store ingest query graph has time-to-live (TTL) component
    primary_entity_ids: List[PydanticObjectId]
    feature_job_setting: Optional[FeatureJobSetting]
    has_ttl: bool

    # pydantic validators
    _sort_ids_validator = validator("primary_entity_ids", allow_reuse=True)(
        construct_sort_validator()
    )

    @classmethod
    def create_from_graph_node(
        cls, graph_node_param: OfflineStoreIngestQueryGraphNodeParameters, ref_node_name: str
    ) -> OfflineStoreIngestQueryGraph:
        """
        Create OfflineStoreIngestQueryGraph from OfflineStoreIngestQueryGraphNodeParameters

        Parameters
        ----------
        graph_node_param: OfflineStoreIngestQueryGraphNodeParameters
            OfflineStoreIngestQueryGraphNodeParameters
        ref_node_name: str
            Node name that refers to the graph node from the decomposed query graph

        Returns
        -------
        OfflineStoreIngestQueryGraph
            OfflineStoreIngestQueryGraph
        """
        return cls(
            graph=graph_node_param.graph,
            node_name=graph_node_param.output_node_name,
            ref_node_name=ref_node_name,
            offline_store_table_name=graph_node_param.offline_store_table_name,
            aggregation_nodes_info=graph_node_param.aggregation_nodes_info,
            output_column_name=graph_node_param.output_column_name,
            output_dtype=graph_node_param.output_dtype,
            primary_entity_ids=graph_node_param.primary_entity_ids,
            feature_job_setting=graph_node_param.feature_job_setting,
            has_ttl=graph_node_param.has_ttl,
        )

    @classmethod
    def create_from_metadata(
        cls, graph: QueryGraphModel, node_name: str, metadata: OfflineStoreInfoMetadata
    ) -> OfflineStoreIngestQueryGraph:
        """
        Create OfflineStoreIngestQueryGraph from OfflineStoreInfoMetadata

        Parameters
        ----------
        graph: QueryGraphModel
            QueryGraphModel
        node_name: str
            Node name that refers to the output node from the query graph
        metadata: OfflineStoreInfoMetadata
            OfflineStoreInfoMetadata

        Returns
        -------
        OfflineStoreIngestQueryGraph
            OfflineStoreIngestQueryGraph
        """
        return cls(
            graph=graph,
            node_name=node_name,
            ref_node_name=None,
            offline_store_table_name=metadata.offline_store_table_name,
            aggregation_nodes_info=metadata.aggregation_nodes_info,
            output_column_name=metadata.output_column_name,
            output_dtype=metadata.output_dtype,
            primary_entity_ids=metadata.primary_entity_ids,
            feature_job_setting=metadata.feature_job_setting,
            has_ttl=metadata.has_ttl,
        )

    def ingest_graph_and_node(self) -> Tuple[QueryGraphModel, Node]:
        """
        Construct graph and node for generating offline store ingest SQL query

        Returns
        -------
        Tuple[QueryGraphModel, Node]
            Ingest graph and node
        """
        output_node = self.graph.get_node_by_name(self.node_name)

        # if the query graph is decomposed, update the graph output column name to match output_column_name
        if output_node.type != NodeType.ALIAS:
            graph = QueryGraphModel(**self.graph.dict(by_alias=True))
        else:
            output_parent_node_name = self.graph.backward_edges_map[self.node_name][0]
            transformer = QuickGraphStructurePruningTransformer(graph=self.graph)
            graph, node_name_map = transformer.transform(
                target_node_names=[output_parent_node_name]
            )
            output_node = graph.get_node_by_name(node_name_map[output_parent_node_name])

        # add alias node to rename the output column name
        output_node = graph.add_operation(
            node_type=NodeType.ALIAS,
            node_params={"name": self.output_column_name},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[output_node],
        )
        return graph, output_node


class OfflineStoreInfo(QueryGraphMixin, FeatureByteBaseModel):
    """
    OfflineStoreInfo object stores the offline store table information of the feature or target.
    It contains the following attributes:
    - graph: decomposed query graph used to generate the offline store table
    - node_name: output node name from the decomposed query graph
    - is_decomposed: whether the feature or target query graph is decomposed
    - metadata: offline store table metadata
    """

    # map the original node name to the decomposed node name
    node_name: str
    node_name_map: Dict[str, str]
    is_decomposed: bool

    # if the feature's or target's query graph is not decomposed, metadata will be populated.
    metadata: Optional[OfflineStoreInfoMetadata]

    def extract_offline_store_ingest_query_graphs(self) -> List[OfflineStoreIngestQueryGraph]:
        """
        Extract offline store ingest query graphs from the feature or target query graph

        Returns
        -------
        List[OfflineStoreIngestQueryGraph]
            List of OfflineStoreIngestQueryGraph
        """
        output = []
        if self.is_decomposed:
            for graph_node in self.graph.iterate_sorted_graph_nodes(
                graph_node_types={GraphNodeType.OFFLINE_STORE_INGEST_QUERY}
            ):
                graph_node_params = graph_node.parameters
                assert isinstance(graph_node_params, OfflineStoreIngestQueryGraphNodeParameters)
                output.append(
                    OfflineStoreIngestQueryGraph.create_from_graph_node(
                        graph_node_param=graph_node_params,
                        ref_node_name=graph_node.name,
                    )
                )
        else:
            assert self.metadata is not None
            output.append(
                OfflineStoreIngestQueryGraph.create_from_metadata(
                    graph=self.graph,
                    node_name=self.node_name,
                    metadata=self.metadata,
                )
            )
        return output

    def generate_on_demand_feature_view_code(
        self,
        feature_name_version: str,
        input_df_name: str = "inputs",
        output_df_name: str = "df",
        function_name: str = "on_demand_feature_view",
        ttl_seconds: Optional[int] = None,
    ) -> str:
        """
        Extract on demand view graphs from the feature or target query graph

        Parameters
        ----------
        feature_name_version: str
            Feature name
        input_df_name: str
            Input dataframe name
        output_df_name: str
            Output dataframe name
        function_name: str
            Function name
        ttl_seconds: Optional[int]
            Time-to-live (TTL) in seconds

        Returns
        -------
        str
            Generated code
        """
        if self.is_decomposed:
            node = self.graph.get_node_by_name(self.node_name)
            codegen_state = OnDemandFeatureViewExtractor(graph=self.graph).extract(
                node=node,
                input_df_name=input_df_name,
                output_df_name=output_df_name,
                on_demand_function_name=function_name,
                ttl_seconds=ttl_seconds,
                feature_name_version=feature_name_version,
            )
            code_generator = codegen_state.code_generator
        else:
            assert ttl_seconds is not None, "TTL is not set"
            code_generator = CodeGenerator(template="on_demand_view.tpl")
            statements = OnDemandFeatureViewExtractor.generate_ttl_handling_statements(
                feature_name_version=feature_name_version,
                input_df_name=input_df_name,
                output_df_name=output_df_name,
                input_column_expr=subset_frame_column_expr(
                    input_df_name,
                    feature_name_version,
                ),
                ttl_seconds=ttl_seconds,
                var_name_generator=VariableNameGenerator(),
            )
            code_generator.add_statements(statements=[statements])

        codes = code_generator.generate(
            input_df_name=input_df_name,
            output_df_name=output_df_name,
            function_name=function_name,
        )
        return codes

    def _generate_on_demand_feature_function_code_state(
        self,
        output_dtype: DBVarType,
        generate_full_code: bool = False,
        sql_function_name: str = "odf_func",
        sql_input_var_prefix: str = "x",
        sql_request_input_var_prefix: str = "r",
        sql_comment: str = "",
        function_name: str = "on_demand_feature_function",
        input_var_prefix: str = "col",
        request_input_var_prefix: str = "request_col",
    ) -> OnDemandFeatureFunctionGlobalState:
        if not self.is_decomposed:
            raise ValueError(
                "Cannot generate on demand feature function code for non-decomposed query graph"
            )

        node = self.graph.get_node_by_name(self.node_name)
        codegen_state = OnDemandFeatureFunctionExtractor(graph=self.graph).extract(
            node=node,
            generate_full_code=generate_full_code,
            sql_function_name=sql_function_name,
            sql_input_var_prefix=sql_input_var_prefix,
            sql_request_input_var_prefix=sql_request_input_var_prefix,
            sql_comment=sql_comment,
            function_name=function_name,
            input_var_prefix=input_var_prefix,
            request_input_var_prefix=request_input_var_prefix,
            output_dtype=output_dtype,
        )
        return codegen_state

    def generate_on_demand_feature_function_code(
        self,
        output_dtype: DBVarType,
        generate_full_code: bool = False,
        sql_function_name: str = "odf_func",
        sql_input_var_prefix: str = "x",
        sql_request_input_var_prefix: str = "r",
        sql_comment: str = "",
        function_name: str = "on_demand_feature_function",
        input_var_prefix: str = "col",
        request_input_var_prefix: str = "request_col",
    ) -> str:
        """
        Generate on demand feature function code

        Parameters
        ----------
        output_dtype: DBVarType
            Output dtype of the on demand feature
        generate_full_code: bool
            Whether to generate full code, full code includes the SQL function code used to register the UDF
        sql_function_name: str
            SQL function name (only used when generate_full_code is True)
        sql_input_var_prefix: str
            SQL input variable prefix (only used when generate_full_code is True)
        sql_request_input_var_prefix: str
            SQL request input variable prefix (only used when generate_full_code is True)
        sql_comment: str
            SQL comment (only used when generate_full_code is True)
        function_name: str
            Python function name
        input_var_prefix: str
            Python input variable prefix
        request_input_var_prefix: str
            Python request input variable prefix

        Returns
        -------
        str
        """
        codegen_state = self._generate_on_demand_feature_function_code_state(
            output_dtype=output_dtype,
            generate_full_code=generate_full_code,
            sql_function_name=sql_function_name,
            sql_input_var_prefix=sql_input_var_prefix,
            sql_request_input_var_prefix=sql_request_input_var_prefix,
            sql_comment=sql_comment,
            function_name=function_name,
            input_var_prefix=input_var_prefix,
            request_input_var_prefix=request_input_var_prefix,
        )
        return codegen_state.generate_code()
