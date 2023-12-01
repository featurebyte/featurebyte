"""
This module contains Feature related models
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from datetime import datetime

import pymongo
from bson import ObjectId
from pydantic import Field, PrivateAttr, root_validator, validator

from featurebyte.common.validator import construct_sort_validator, version_validator
from featurebyte.enum import DBVarType
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
    VersionIdentifier,
)
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSetting,
    TableFeatureJobSetting,
    TableIdFeatureJobSetting,
)
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.cleaning_operation import (
    TableCleaningOperation,
    TableIdCleaningOperation,
)
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure
from featurebyte.query_graph.node.nested import AggregationNodeInfo
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.online_store_compute_query import (
    get_online_store_precompute_queries,
)
from featurebyte.query_graph.transform.definition import (
    DefinitionHashExtractor,
    DefinitionHashOutput,
)
from featurebyte.query_graph.transform.offline_ingest_extractor import (
    OfflineStoreIngestQueryGraphExtractor,
)
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.transform.quick_pruning import QuickGraphStructurePruningTransformer


class TableIdColumnNames(FeatureByteBaseModel):
    """
    TableIdColumnNames object stores the table id and the column names of the table that are used by
    the feature or target.
    """

    table_id: PydanticObjectId
    column_names: List[str]


class OfflineStoreIngestQueryGraph(FeatureByteBaseModel):
    """
    OfflineStoreIngestQuery object stores the offline store ingest query for a feature or target.
    """

    # offline store ingest query graph & output node name (from the graph)
    graph: QueryGraphModel
    node_name: str
    # primary entity ids of the offline store ingest query graph
    primary_entity_ids: List[PydanticObjectId]
    # reference node name that is used in decomposed query graph
    # if None, the query graph is not decomposed
    ref_node_name: Optional[str]
    # output column name of the offline store ingest query graph
    output_column_name: str
    output_dtype: DBVarType
    # feature job setting of the offline store ingest query graph
    feature_job_setting: Optional[FeatureJobSetting]
    # whether the offline store ingest query graph has time-to-live (TTL) component
    has_ttl: bool
    # aggregation nodes info of the offline store ingest query graph
    aggregation_nodes_info: List[AggregationNodeInfo]

    def ingest_graph_and_node(self) -> Tuple[QueryGraphModel, Node]:
        """
        Construct graph and node for generating offline store ingest SQL query

        Returns
        -------
        Tuple[QueryGraphModel, Node]
            Ingest graph and node
        """
        output_node = self.graph.get_node_by_name(self.node_name)
        if self.ref_node_name is None:
            # if the query graph is not decomposed, return the original graph & node
            return self.graph, output_node

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


class BaseFeatureModel(FeatureByteCatalogBaseDocumentModel):
    """
    BaseFeatureModel is the base class for FeatureModel & TargetModel.
    It contains all the attributes that are shared between FeatureModel & TargetModel.
    """

    dtype: DBVarType = Field(allow_mutation=False, default=DBVarType.UNKNOWN)
    node_name: str
    tabular_source: TabularSource = Field(allow_mutation=False)
    version: VersionIdentifier = Field(allow_mutation=False, default=None)
    definition: Optional[str] = Field(allow_mutation=False, default=None)
    definition_hash: Optional[str] = Field(allow_mutation=False, default=None)

    # special handling for those attributes that are expensive to deserialize
    # internal_* is used to store the raw data from persistence, _* is used as a cache
    internal_graph: Any = Field(allow_mutation=False, alias="graph")
    _graph: Optional[QueryGraph] = PrivateAttr(default=None)

    # query graph derived attributes
    # - table columns used by the feature or target
    # - table feature job settings used by the feature or target
    # - table cleaning operations used by the feature or target
    table_id_column_names: List[TableIdColumnNames] = Field(
        allow_mutation=False, default_factory=list
    )
    table_id_feature_job_settings: List[TableIdFeatureJobSetting] = Field(
        allow_mutation=False, default_factory=list
    )
    table_id_cleaning_operations: List[TableIdCleaningOperation] = Field(
        allow_mutation=False, default_factory=list
    )

    # list of IDs attached to this feature or target
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    primary_entity_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    table_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    primary_table_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    user_defined_function_ids: List[PydanticObjectId] = Field(
        allow_mutation=False, default_factory=list
    )

    # relationship info contains the bare enough entity relationship information between all the entities
    # for example, if there are following entity relationship (child -> parent):
    # transaction -> order -> customer -> city -> state
    # if the feature uses order & city entities, the relationship info will be (order -> customer -> city)
    # transaction and state will not be included as they are not used by the feature.
    relationships_info: Optional[List[EntityRelationshipInfo]] = Field(
        allow_mutation=False, default=None
    )

    # pydantic validators
    _version_validator = validator("version", pre=True, allow_reuse=True)(version_validator)
    _sort_ids_validator = validator(
        "table_ids",
        "primary_table_ids",
        "entity_ids",
        "primary_entity_ids",
        "user_defined_function_ids",
        allow_reuse=True,
    )(construct_sort_validator())

    @staticmethod
    def _extract_dtype_from_graph(graph: QueryGraphModel, node_name: str) -> DBVarType:
        node = graph.get_node_by_name(node_name)
        op_struct_info = OperationStructureExtractor(graph=graph).extract(
            node=node,
            keep_all_source_columns=True,
        )
        op_struct = op_struct_info.operation_structure_map[node.name]
        if len(op_struct.aggregations) != 1:
            raise ValueError("Feature or target graph must have exactly one aggregation output")
        return op_struct.aggregations[0].dtype

    @root_validator
    @classmethod
    def _add_derived_attributes(cls, values: dict[str, Any]) -> dict[str, Any]:
        # do not check entity_ids as the derived result can be an empty list
        derived_attributes = [
            values.get("primary_table_ids"),
            values.get("table_ids"),
            values.get("dtype"),
            values.get("table_id_column_names"),
        ]
        if any(not x for x in derived_attributes):
            # only derive attributes if any of them is missing
            # extract table ids & entity ids from the graph
            graph_dict = values["internal_graph"]
            if isinstance(graph_dict, QueryGraphModel):
                graph_dict = graph_dict.dict(by_alias=True)
            graph = QueryGraph(**graph_dict)
            node_name = values["node_name"]
            values["primary_table_ids"] = graph.get_primary_table_ids(node_name=node_name)
            values["table_ids"] = graph.get_table_ids(node_name=node_name)
            values["entity_ids"] = graph.get_entity_ids(node_name=node_name)
            values["user_defined_function_ids"] = graph.get_user_defined_function_ids(
                node_name=node_name
            )

            # extract table feature job settings, table cleaning operations, table column names
            node = graph.get_node_by_name(node_name)
            table_id_to_col_names = graph.extract_table_id_to_table_column_names(node=node)
            values["table_id_column_names"] = [
                TableIdColumnNames(
                    table_id=table_id,
                    column_names=sorted(table_id_to_col_names[table_id]),
                )
                for table_id in sorted(table_id_to_col_names)
            ]
            values["table_id_feature_job_settings"] = graph.extract_table_id_feature_job_settings(
                target_node=node, keep_first_only=True
            )
            values["table_id_cleaning_operations"] = graph.extract_table_id_cleaning_operations(
                target_node=node,
                keep_all_columns=True,
                table_id_to_col_names=table_id_to_col_names,
            )

            # extract dtype from the graph
            values["dtype"] = cls._extract_dtype_from_graph(graph, node_name)

        return values

    @validator("name")
    @classmethod
    def _validate_asset_name(cls, value: Optional[str]) -> Optional[str]:
        if value and value.startswith("__"):
            raise ValueError(
                f"{cls.__name__} name cannot start with '__' as it is reserved for internal use."
            )
        return value

    @validator(
        "table_id_column_names", "table_id_feature_job_settings", "table_id_cleaning_operations"
    )
    @classmethod
    def _sort_list_by_table_id_(cls, value: List[Any]) -> List[Any]:
        return sorted(value, key=lambda item: item.table_id)  # type: ignore

    @property
    def node(self) -> Node:
        """
        Retrieve node

        Returns
        -------
        Node
            Node object
        """

        return self.graph.get_node_by_name(self.node_name)

    @property
    def graph(self) -> QueryGraph:
        """
        Get the graph. If the graph is not loaded, load it first.

        Returns
        -------
        QueryGraph
            QueryGraph object
        """
        # TODO: make this a cached_property for pydantic v2
        if self._graph is None:
            if isinstance(self.internal_graph, QueryGraph):
                self._graph = self.internal_graph
            else:
                if isinstance(self.internal_graph, dict):
                    graph_dict = self.internal_graph
                else:
                    # for example, QueryGraphModel
                    graph_dict = self.internal_graph.dict(by_alias=True)
                self._graph = QueryGraph(**graph_dict)
        return self._graph

    def extract_pruned_graph_and_node(self, **kwargs: Any) -> tuple[QueryGraphModel, Node]:
        """
        Extract pruned graph and node

        Parameters
        ----------
        **kwargs: Any
            Additional keyword parameters

        Returns
        -------
        tuple[QueryGraph, Node]
            Pruned graph and node
        """
        _ = kwargs
        pruned_graph, node_name_map = self.graph.prune(target_node=self.node)
        mapped_node = pruned_graph.get_node_by_name(node_name_map[self.node.name])
        return pruned_graph, mapped_node

    def extract_operation_structure(
        self, keep_all_source_columns: bool = False
    ) -> GroupOperationStructure:
        """
        Extract feature or target operation structure based on query graph. This method is mainly
        used for deriving feature or target metadata used in feature/target info.

        Parameters
        ----------
        keep_all_source_columns: bool
            Whether to keep all source columns in the operation structure

        Returns
        -------
        GroupOperationStructure
        """
        # group the view columns by source columns & derived columns
        operation_structure = self.graph.extract_operation_structure(
            self.node, keep_all_source_columns=keep_all_source_columns
        )
        return operation_structure.to_group_operation_structure()

    def extract_table_id_feature_job_settings(
        self, keep_first_only: bool = False
    ) -> List[TableIdFeatureJobSetting]:
        """
        Extract table id feature job settings

        Parameters
        ----------
        keep_first_only: bool
            Whether to keep only the first table id feature job setting

        Returns
        -------
        List[TableIdFeatureJobSetting]
            List of table id feature job settings
        """
        table_id_feature_job_settings = self.graph.extract_table_id_feature_job_settings(
            target_node=self.node
        )
        if not keep_first_only:
            return table_id_feature_job_settings

        output = []
        found_table_ids = set()
        for setting in table_id_feature_job_settings:
            if setting.table_id not in found_table_ids:
                output.append(setting)
                found_table_ids.add(setting.table_id)
        return output

    def extract_table_feature_job_settings(
        self, table_id_to_name: Dict[ObjectId, str], keep_first_only: bool = False
    ) -> List[TableFeatureJobSetting]:
        """
        Extract table feature job settings

        Parameters
        ----------
        table_id_to_name: Dict[ObjectId, str]
            Table id to table name mapping
        keep_first_only: bool
            Whether to keep only the first table feature job setting

        Returns
        -------
        List[TableFeatureJobSetting]
            List of table feature job settings
        """
        output = []
        for setting in self.extract_table_id_feature_job_settings(keep_first_only=keep_first_only):
            output.append(
                TableFeatureJobSetting(
                    table_name=table_id_to_name[setting.table_id],
                    feature_job_setting=setting.feature_job_setting,
                )
            )
        return output

    def extract_table_id_cleaning_operations(
        self, keep_all_columns: bool = True
    ) -> List[TableIdCleaningOperation]:
        """
        Extract table cleaning operations

        Parameters
        ----------
        keep_all_columns: bool
            Whether to keep all columns

        Returns
        -------
        List[TableIdCleaningOperation]
            List of table cleaning operations
        """
        return self.graph.extract_table_id_cleaning_operations(
            target_node=self.node, keep_all_columns=keep_all_columns
        )

    def extract_table_cleaning_operations(
        self, table_id_to_name: Dict[ObjectId, str], keep_all_columns: bool = True
    ) -> List[TableCleaningOperation]:
        """
        Extract table cleaning operations

        Parameters
        ----------
        table_id_to_name: Dict[ObjectId, str]
            Table id to table name mapping
        keep_all_columns: bool
            Whether to keep all columns

        Returns
        -------
        List[TableCleaningOperation]
            List of table cleaning operations
        """
        output = []
        for cleaning_operation in self.extract_table_id_cleaning_operations(
            keep_all_columns=keep_all_columns
        ):
            output.append(
                TableCleaningOperation(
                    table_name=table_id_to_name[cleaning_operation.table_id],
                    column_cleaning_operations=cleaning_operation.column_cleaning_operations,
                )
            )
        return output

    def extract_definition_hash(self) -> DefinitionHashOutput:
        """
        Extract definition hash

        Returns
        -------
        DefinitionHashOutput
            Definition hash output
        """
        extractor = DefinitionHashExtractor(graph=self.graph)
        return extractor.extract(self.node)

    def _extract_aggregation_nodes_info(self) -> List[AggregationNodeInfo]:
        operation_structure = self.graph.extract_operation_structure(self.node)
        output = []
        for agg in operation_structure.iterate_aggregations():
            node = self.graph.get_node_by_name(agg.node_name)
            input_node_names = self.graph.backward_edges_map[node.name]
            assert len(input_node_names) <= 1
            output.append(
                AggregationNodeInfo(
                    node_type=node.type,
                    input_node_name=input_node_names[0] if input_node_names else None,
                    node_name=node.name,
                )
            )
        return output

    def extract_offline_store_ingest_query_graphs(self) -> List[OfflineStoreIngestQueryGraph]:
        """
        Extract offline store ingest query graphs

        Returns
        -------
        List[OfflineStoreIngestQueryGraph]
            List of offline store ingest query graphs
        """
        extractor = OfflineStoreIngestQueryGraphExtractor(graph=self.graph)
        assert self.name is not None
        result = extractor.extract(
            node=self.node, relationships_info=self.relationships_info, feature_name=self.name
        )
        output = []
        if result.is_decomposed:
            for graph_node in result.graph.iterate_sorted_graph_nodes(
                graph_node_types={GraphNodeType.OFFLINE_STORE_INGEST_QUERY}
            ):
                exit_node_name = result.graph_node_name_to_exit_node_name[graph_node.name]
                graph_node_params = graph_node.parameters
                aggregation_info = result.node_name_to_aggregation_info[exit_node_name]
                output.append(
                    OfflineStoreIngestQueryGraph(
                        graph=graph_node.parameters.graph,
                        node_name=graph_node.parameters.output_node_name,
                        primary_entity_ids=aggregation_info.primary_entity_ids,
                        ref_node_name=graph_node.name,
                        output_column_name=graph_node_params.output_column_name,  # type: ignore
                        output_dtype=self._extract_dtype_from_graph(
                            graph=graph_node.parameters.graph,
                            node_name=graph_node.parameters.output_node_name,
                        ),
                        feature_job_setting=graph_node_params.feature_job_setting,  # type: ignore
                        has_ttl=aggregation_info.has_ttl_agg_type,
                        aggregation_nodes_info=graph_node_params.aggregation_nodes_info,  # type: ignore
                    )
                )
        else:
            feature_job_setting = None
            if self.table_id_feature_job_settings:
                feature_job_setting = self.table_id_feature_job_settings[0].feature_job_setting

            output.append(
                OfflineStoreIngestQueryGraph(
                    graph=self.graph,
                    node_name=self.node_name,
                    primary_entity_ids=self.primary_entity_ids,
                    ref_node_name=None,
                    output_column_name=self.name,
                    output_dtype=self._extract_dtype_from_graph(
                        graph=self.graph, node_name=self.node_name
                    ),
                    feature_job_setting=feature_job_setting,
                    has_ttl=bool(
                        # check if there is a GroupByNode in the graph
                        next(
                            self.graph.iterate_nodes(
                                target_node=self.node, node_type=NodeType.GROUPBY
                            ),
                            None,
                        )
                    ),
                    aggregation_nodes_info=self._extract_aggregation_nodes_info(),
                )
            )

        return output

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        unique_constraints = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
            UniqueValuesConstraint(
                fields=("name", "version"),
                conflict_fields_signature={"name": ["name"], "version": ["version"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("dtype"),
            pymongo.operations.IndexModel("version"),
            pymongo.operations.IndexModel("entity_ids"),
            pymongo.operations.IndexModel("primary_entity_ids"),
            pymongo.operations.IndexModel("table_ids"),
            pymongo.operations.IndexModel("primary_table_ids"),
            pymongo.operations.IndexModel("user_defined_function_ids"),
            pymongo.operations.IndexModel("relationships_info"),
            pymongo.operations.IndexModel(
                [
                    ("name", pymongo.TEXT),
                    ("version", pymongo.TEXT),
                    ("description", pymongo.TEXT),
                ],
            ),
        ]


class FeatureModel(BaseFeatureModel):
    """
    Model for Feature asset

    id: PydanticObjectId
        Feature id of the object
    name: str
        Feature name
    dtype: DBVarType
        Variable type of the feature
    graph: QueryGraph
        Graph contains steps of transformation to generate the feature
    node_name: str
        Node name of the graph which represent the feature
    tabular_source: TabularSource
        Tabular source used to construct this feature
    readiness: FeatureReadiness
        Feature readiness
    version: VersionIdentifier
        Feature version
    online_enabled: bool
        Whether to make this feature version online enabled
    definition: str
        Feature definition
    entity_ids: List[PydanticObjectId]
        Entity IDs used by the feature
    table_ids: List[PydanticObjectId]
        Table IDs used by the feature
    primary_table_ids: Optional[List[PydanticObjectId]]
        Primary table IDs of the feature (auto-derive from graph)
    feature_namespace_id: PydanticObjectId
        Feature namespace id of the object
    feature_list_ids: List[PydanticObjectId]
        FeatureList versions which use this feature version
    deployed_feature_list_ids: List[PydanticObjectId]
        Deployed FeatureList versions which use this feature version
    created_at: Optional[datetime]
        Datetime when the Feature was first saved
    updated_at: Optional[datetime]
        When the Feature get updated
    last_updated_by_scheduled_task_at: Optional[datetime]
        Datetime when the Feature value was last updated
    """

    readiness: FeatureReadiness = Field(allow_mutation=False, default=FeatureReadiness.DRAFT)
    online_enabled: bool = Field(allow_mutation=False, default=False)

    # ID related fields associated with this feature
    feature_namespace_id: PydanticObjectId = Field(allow_mutation=False, default_factory=ObjectId)
    feature_list_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    deployed_feature_list_ids: List[PydanticObjectId] = Field(
        allow_mutation=False, default_factory=list
    )
    aggregation_ids: List[str] = Field(allow_mutation=False, default_factory=list)
    aggregation_result_names: List[str] = Field(allow_mutation=False, default_factory=list)
    online_store_table_names: List[str] = Field(allow_mutation=False, default_factory=list)
    last_updated_by_scheduled_task_at: Optional[datetime] = Field(
        allow_mutation=False, default=None
    )

    @root_validator
    @classmethod
    def _add_tile_derived_attributes(cls, values: dict[str, Any]) -> dict[str, Any]:
        # Each aggregation_id refers to a set of columns in a tile table. It is associated to a
        # specific scheduled tile task. An aggregation_id can produce multiple aggregation results
        # using different feature derivation windows.
        if values.get("aggregation_ids") and values.get("aggregation_result_names"):
            return values

        graph_dict = values["internal_graph"]
        if isinstance(graph_dict, QueryGraphModel):
            graph_dict = graph_dict.dict(by_alias=True)
        graph = QueryGraph(**graph_dict)
        node_name = values["node_name"]
        feature_store_type = graph.get_input_node(node_name).parameters.feature_store_details.type

        interpreter = GraphInterpreter(graph, feature_store_type)
        node = graph.get_node_by_name(node_name)

        try:
            tile_infos = interpreter.construct_tile_gen_sql(node, is_on_demand=False)
        except StopIteration:
            # add a try except block here for the old features that may trigger StopIteration,
            # in this case, we will not add tile related attributes
            return values

        aggregation_ids = []
        for info in tile_infos:
            aggregation_ids.append(info.aggregation_id)

        values["aggregation_ids"] = aggregation_ids

        values["aggregation_result_names"] = []
        online_store_table_names = set()
        for query in get_online_store_precompute_queries(
            graph, graph.get_node_by_name(node_name), feature_store_type
        ):
            values["aggregation_result_names"].append(query.result_name)
            online_store_table_names.add(query.table_name)
        values["online_store_table_names"] = sorted(online_store_table_names)

        return values

    class Settings(BaseFeatureModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature"
        indexes = BaseFeatureModel.Settings.indexes + [
            pymongo.operations.IndexModel("readiness"),
            pymongo.operations.IndexModel("online_enabled"),
            pymongo.operations.IndexModel("feature_namespace_id"),
            pymongo.operations.IndexModel("feature_list_ids"),
            pymongo.operations.IndexModel("deployed_feature_list_ids"),
            pymongo.operations.IndexModel("aggregation_ids"),
            pymongo.operations.IndexModel("aggregation_result_names"),
            pymongo.operations.IndexModel("online_store_table_names"),
        ]
