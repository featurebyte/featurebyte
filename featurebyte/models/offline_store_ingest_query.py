"""
OfflineStoreIngestQuery object stores the offline store ingest query for a feature.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Dict, List, Optional, Tuple

from bson import ObjectId
from pydantic import Field, field_validator
from pydantic_extra_types.timezone_name import TimeZoneName

from featurebyte.common.string import sanitize_identifier
from featurebyte.common.validator import construct_sort_validator
from featurebyte.enum import DBVarType, SourceType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.mixin import QueryGraphMixin
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.model.feature_job_setting import (
    CronFeatureJobSetting,
    FeatureJobSettingUnion,
)
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.config import OnDemandFunctionCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    StatementStr,
    ValueStr,
    VariableNameGenerator,
)
from featurebyte.query_graph.node.nested import (
    AggregationNodeInfo,
    OfflineStoreIngestQueryGraphNodeParameters,
    OfflineStoreMetadata,
)
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.query_graph.node.utils import subset_frame_column_expr
from featurebyte.query_graph.transform.on_demand_function import (
    OnDemandFeatureFunctionExtractor,
    OnDemandFeatureFunctionGlobalState,
    SQLInputArgumentInfo,
)
from featurebyte.query_graph.transform.on_demand_view import OnDemandFeatureViewExtractor
from featurebyte.query_graph.transform.quick_pruning import QuickGraphStructurePruningTransformer
from featurebyte.typing import Scalar


class OfflineStoreInfoMetadata(OfflineStoreMetadata):
    """
    OfflineStoreInfoMetadata object stores the offline store table metadata of the feature or target.
    """

    output_column_name: str
    primary_entity_ids: List[PydanticObjectId]


class OfflineStoreEntityInfo(ColumnSpec):
    """
    EntityInfo object stores the entity information of the feature.
    """

    id: PydanticObjectId


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
    ref_node_name: Optional[str] = Field(default=None)
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
    primary_entity_dtypes: List[DBVarType]
    feature_job_setting: Optional[FeatureJobSettingUnion] = Field(default=None)
    has_ttl: bool

    # pydantic validators
    _sort_ids_validator = field_validator("primary_entity_ids")(construct_sort_validator())

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
            primary_entity_dtypes=graph_node_param.primary_entity_dtypes,
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
            primary_entity_dtypes=metadata.primary_entity_dtypes,
            feature_job_setting=metadata.feature_job_setting,
            has_ttl=metadata.has_ttl,
        )

    def get_primary_entity_info(
        self, entity_id_to_serving_name: Dict[PydanticObjectId, str]
    ) -> List[OfflineStoreEntityInfo]:
        """
        Get primary entity info of the offline store ingest query graph

        Parameters
        ----------
        entity_id_to_serving_name: Dict[PydanticObjectId, str]
            Map from entity id to serving name

        Returns
        -------
        List[OfflineStoreEntityInfo]
            List of EntityInfo
        """
        output = []
        for entity_id, entity_dtype in zip(self.primary_entity_ids, self.primary_entity_dtypes):
            serving_name = entity_id_to_serving_name[entity_id]
            output.append(
                OfflineStoreEntityInfo(
                    id=entity_id,
                    name=serving_name,
                    dtype=entity_dtype,
                )
            )
        return output

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
            graph = QueryGraphModel(**self.graph.model_dump(by_alias=True))
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


class ServingNameInfo(FeatureByteBaseModel):
    """
    ServingNameInfo object stores the serving name information of the feature.
    """

    serving_name: str
    entity_id: PydanticObjectId


class OnDemandFeatureViewInfo(FeatureByteBaseModel):
    """
    OnDemandFeatureViewInfo object stores the on configuration used to generate the on demand feature view code.
    """

    feature_versioned_name: str
    input_df_name: str
    output_df_name: str
    function_name: str
    codes: str = Field(default="")


class UserDefinedFunctionInfo(FeatureByteBaseModel):
    """
    UserDefinedFunctionInfo object stores the user defined function information of the feature.
    """

    sql_function_name: str
    sql_input_var_prefix: str
    sql_request_input_var_prefix: str
    function_name: str
    input_var_prefix: str
    request_input_var_prefix: str
    sql_inputs_info: List[SQLInputArgumentInfo] = Field(default_factory=list)
    codes: str = Field(default="")


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
    metadata: Optional[OfflineStoreInfoMetadata] = Field(default=None)

    # list of on demand feature codes that are used by the feature or target when the feature is online-enabled
    serving_names_info: List[ServingNameInfo] = Field(default_factory=list)
    time_to_live_in_secs: Optional[int] = Field(default=None)
    cron_expression: Optional[str] = Field(default=None)
    cron_timezone: Optional[TimeZoneName] = Field(default=None)
    null_filling_value: Optional[Scalar] = Field(default=None)
    odfv_info: Optional[OnDemandFeatureViewInfo] = Field(default=None)
    udf_info: Optional[UserDefinedFunctionInfo] = Field(default=None)

    @property
    def time_to_live_delta(self) -> Optional[timedelta]:
        """
        Time-to-live (TTL) in timedelta

        Returns
        -------
        Optional[timedelta]
            Time-to-live (TTL) in timedelta
        """
        if not self.time_to_live_in_secs:
            return None
        return timedelta(seconds=self.time_to_live_in_secs)

    def initialize(
        self,
        feature_versioned_name: str,
        feature_dtype: DBVarType,
        feature_job_settings: List[FeatureJobSettingUnion],
        feature_id: ObjectId,
        has_ttl: bool,
        source_type: SourceType,
        null_filling_value: Optional[Scalar] = None,
    ) -> None:
        """
        Initialize offline store info by populating the on demand feature view info and user defined function info

        Parameters
        ----------
        feature_versioned_name: str
            Feature versioned name
        feature_dtype: DBVarType
            Output dtype of the feature
        feature_job_settings: List[FeatureJobSettingUnion]
            List of feature job settings used by the feature
        feature_id: ObjectId
            Feature ID
        has_ttl: bool
            Whether the feature has time-to-live (TTL) component
        source_type: SourceType
            Source type of the feature
        null_filling_value: Optional[Scalar]
            Null filling value
        """
        self.time_to_live_in_secs = None
        self.null_filling_value = null_filling_value
        if has_ttl and feature_job_settings:
            self.time_to_live_in_secs = min(
                feature_job_setting.extract_ttl_seconds()
                for feature_job_setting in feature_job_settings
            )

            if isinstance(feature_job_settings[0], CronFeatureJobSetting):
                self.cron_expression = feature_job_settings[0].get_cron_expression()
                self.cron_timezone = feature_job_settings[0].timezone

        unique_func_name = f"{sanitize_identifier(feature_versioned_name)}_{feature_id}"
        if self.is_decomposed or self.time_to_live_in_secs or self.null_filling_value is not None:
            # initialize the on demand feature view info
            odfv_info = OnDemandFeatureViewInfo(
                feature_versioned_name=feature_versioned_name,
                input_df_name="inputs",
                output_df_name="df",
                function_name=f"odfv_{unique_func_name}",
            )
            odfv_info.codes = self.generate_on_demand_feature_view_code(
                feature_versioned_name=feature_versioned_name,
                source_type=source_type,
                input_df_name=odfv_info.input_df_name,
                output_df_name=odfv_info.output_df_name,
                function_name=odfv_info.function_name,
                ttl_seconds=self.time_to_live_in_secs,
            )
            self.odfv_info = odfv_info

        if self.is_decomposed or self.null_filling_value is not None:
            # initialize the user defined function info
            udf_info = UserDefinedFunctionInfo(
                sql_function_name=f"udf_{unique_func_name}",
                sql_input_var_prefix="x",
                sql_request_input_var_prefix="r",
                function_name="user_defined_function",
                input_var_prefix="col",
                request_input_var_prefix="request_col",
            )
            udf_code_state = self._generate_databricks_user_defined_function_code(
                output_dtype=feature_dtype,
                sql_function_name=udf_info.sql_function_name,
                sql_input_var_prefix=udf_info.sql_input_var_prefix,
                sql_request_input_var_prefix=udf_info.sql_request_input_var_prefix,
                function_name=udf_info.function_name,
                input_var_prefix=udf_info.input_var_prefix,
                request_input_var_prefix=udf_info.request_input_var_prefix,
            )
            udf_info.sql_inputs_info = udf_code_state.sql_inputs_info
            udf_info.codes = udf_code_state.generate_code(to_sql=True)
            self.udf_info = udf_info

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
        feature_versioned_name: str,
        source_type: SourceType,
        input_df_name: str = "inputs",
        output_df_name: str = "df",
        function_name: str = "on_demand_feature_view",
        ttl_seconds: Optional[int] = None,
    ) -> str:
        """
        Extract on demand view graphs from the feature or target query graph

        Parameters
        ----------
        feature_versioned_name: str
            Feature name
        source_type: SourceType
            Source type of the feature
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
                feature_name_version=feature_versioned_name,
                source_type=source_type,
            )
            code_generator = codegen_state.code_generator
        else:
            code_generator = CodeGenerator(template="on_demand_view.tpl")
            assert ttl_seconds is not None or self.null_filling_value is not None
            if self.null_filling_value is not None:
                statements = OnDemandFeatureViewExtractor.generate_null_filling_statements(
                    feature_name_version=feature_versioned_name,
                    output_df_name=output_df_name,
                    input_column_expr=subset_frame_column_expr(
                        input_df_name,
                        feature_versioned_name,
                    ),
                    fill_value=self.null_filling_value,
                )
                code_generator.add_statements(statements=[statements])

            if ttl_seconds is not None:
                statements = OnDemandFeatureViewExtractor.generate_ttl_handling_statements(
                    feature_name_version=feature_versioned_name,
                    input_df_name=input_df_name,
                    output_df_name=output_df_name,
                    ttl_seconds=ttl_seconds,
                    var_name_generator=VariableNameGenerator(),
                    cron_expression=self.cron_expression,
                    cron_timezone=self.cron_timezone,
                    comment="# Time-to-live (TTL) handling to clean up expired data",
                )
                code_generator.add_statements(statements=[statements])

        codes = code_generator.generate(
            to_format=True,
            input_df_name=input_df_name,
            output_df_name=output_df_name,
            function_name=function_name,
        )
        return codes

    def _generate_databricks_user_defined_function_code(
        self,
        output_dtype: DBVarType,
        sql_function_name: str = "udf_func",
        sql_input_var_prefix: str = "x",
        sql_request_input_var_prefix: str = "r",
        sql_comment: str = "",
        function_name: str = "user_defined_function",
        input_var_prefix: str = "col",
        request_input_var_prefix: str = "request_col",
    ) -> OnDemandFeatureFunctionGlobalState:
        codegen_kwargs = {
            "sql_function_name": sql_function_name,
            "sql_input_var_prefix": sql_input_var_prefix,
            "sql_request_input_var_prefix": sql_request_input_var_prefix,
            "sql_comment": sql_comment,
            "function_name": function_name,
            "input_var_prefix": input_var_prefix,
            "request_input_var_prefix": request_input_var_prefix,
            "output_dtype": output_dtype,
        }
        if self.is_decomposed:
            node = self.graph.get_node_by_name(self.node_name)
            codegen_state = OnDemandFeatureFunctionExtractor(graph=self.graph).extract(
                node=node, **codegen_kwargs
            )
        else:
            assert self.null_filling_value is not None
            assert self.metadata is not None, "non-decomposed query graph must have metadata"
            codegen_state = OnDemandFeatureFunctionGlobalState(
                code_generation_config=OnDemandFunctionCodeGenConfig(**codegen_kwargs),
                var_name_generator=VariableNameGenerator(one_based=True),
                # this is a dummy value, it should not be used by the following code
                node_name_to_operation_structure={},
            )
            col = codegen_state.var_name_generator.convert_to_variable_name(
                variable_name_prefix=codegen_state.code_generation_config.input_var_prefix,
                node_name=None,
            )
            codegen_state.register_input_argument(
                variable_name_prefix=codegen_state.code_generation_config.input_var_prefix,
                py_type=codegen_state.code_generation_config.to_py_type(DBVarType(output_dtype)),
                column_name=self.metadata.output_column_name,
            )
            fill_value_expr = ValueStr(self.null_filling_value).as_input()
            codegen_state.code_generator.add_statements(
                statements=[
                    StatementStr(f"return {fill_value_expr} if pd.isnull({col}) else {col}")
                ]
            )

        return codegen_state

    def generate_databricks_user_defined_function_code(
        self,
        output_dtype: DBVarType,
        to_sql: bool = False,
        sql_function_name: str = "udf_func",
        sql_input_var_prefix: str = "x",
        sql_request_input_var_prefix: str = "r",
        sql_comment: str = "",
        function_name: str = "user_defined_function",
        input_var_prefix: str = "col",
        request_input_var_prefix: str = "request_col",
    ) -> str:
        """
        Generate on demand feature function code

        Parameters
        ----------
        output_dtype: DBVarType
            Output dtype of the on demand feature
        to_sql: bool
            Whether to generate SQL code (True) or Python code (False)
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
        codegen_state = self._generate_databricks_user_defined_function_code(
            output_dtype=output_dtype,
            sql_function_name=sql_function_name,
            sql_input_var_prefix=sql_input_var_prefix,
            sql_request_input_var_prefix=sql_request_input_var_prefix,
            sql_comment=sql_comment,
            function_name=function_name,
            input_var_prefix=input_var_prefix,
            request_input_var_prefix=request_input_var_prefix,
        )
        return codegen_state.generate_code(to_sql=to_sql)
