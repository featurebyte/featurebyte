"""
VersionService class
"""
from __future__ import annotations

from typing import Any, List, Optional, Tuple

from bson.objectid import ObjectId

from featurebyte.exception import DocumentError, GraphInconsistencyError
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel, FeatureListNewVersionMode
from featurebyte.persistent import Persistent
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupByNode
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import (
    BaseGraphNode,
    ColumnCleaningOperation,
    DataCleaningOperation,
    ViewMetadata,
)
from featurebyte.schema.feature import FeatureCreate, FeatureNewVersionCreate
from featurebyte.schema.feature_list import FeatureListCreate, FeatureListNewVersionCreate
from featurebyte.service.base_service import BaseService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.tabular_data import DataService


class VersionService(BaseService):
    """
    VersionService class is responsible for creating new feature version
    """

    def __init__(self, user: Any, persistent: Persistent, workspace_id: ObjectId):
        super().__init__(user, persistent, workspace_id)
        self.feature_service = FeatureService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.feature_namespace_service = FeatureNamespaceService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.feature_list_service = FeatureListService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.feature_list_namespace_service = FeatureListNamespaceService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.data_service = DataService(user=user, persistent=persistent, workspace_id=workspace_id)

    @staticmethod
    def _prepare_group_by_node_name_to_replacement_node(
        feature: FeatureModel, feature_job_setting: Optional[FeatureJobSetting]
    ) -> dict[str, Node]:
        """
        Prepare group by node name to replacement node mapping by using the provided feature job setting
        to construct new group by node. If the new group by node is the same as the existing one, then
        the mapping will be empty.

        Parameters
        ----------
        feature: FeatureModel
            Feature model
        feature_job_setting: Optional[FeatureJobSetting]
            Feature job setting

        Returns
        -------
        dict[str, Node]
        """
        node_name_to_replacement_node: dict[str, Node] = {}
        if feature_job_setting:
            for group_by_node, _ in feature.graph.iterate_group_by_and_event_data_input_node_pairs(
                target_node=feature.node
            ):
                # input node will be used when we need to support updating specific groupby node given event data ID
                parameters = {**group_by_node.parameters.dict(), **feature_job_setting.to_seconds()}
                if group_by_node.parameters.dict() != parameters:
                    node_name_to_replacement_node[group_by_node.name] = GroupByNode(
                        **{**group_by_node.dict(), "parameters": parameters}
                    )

        return node_name_to_replacement_node

    @staticmethod
    def _get_create_view_graph_node_keyword_parameters(
        graph_node: BaseGraphNode,
        input_node_names: list[str],
        view_node_name_to_data_info: dict[str, tuple[Node, List[ColumnInfo], InputNode]],
    ) -> dict[str, Any]:
        """
        Get additional parameters used to construct view graph node.

        Parameters
        ----------
        graph_node: BaseGraphNode
            Graph node
        input_node_names: list[str]
            Input node names
        view_node_name_to_data_info: dict[str, tuple[Node, List[ColumnInfo], InputNode]]
            View node name to data info mapping

        Returns
        -------
        dict[str, Any]

        Raises
        ------
        GraphInconsistencyError
            If the graph has unexpected structure
        """
        parameters: dict[str, Any] = {}
        if graph_node.parameters.type == GraphNodeType.ITEM_VIEW:
            # prepare the event view graph node and its columns info for item view graph node to use
            # handle the following assumption so that it won't throw internal server error
            if len(input_node_names) != 2:
                raise GraphInconsistencyError("Item view graph node must have 2 input nodes.")

            event_view_graph_node_name = input_node_names[1]
            if event_view_graph_node_name not in view_node_name_to_data_info:
                raise GraphInconsistencyError(
                    "Event view graph node must be processed before item view graph node."
                )

            # retrieve the processed graph node and its columns info
            (
                event_view_node,
                event_view_columns_info,
                data_input_node,
            ) = view_node_name_to_data_info[event_view_graph_node_name]
            parameters["event_view_node"] = event_view_node
            parameters["event_view_columns_info"] = event_view_columns_info
            parameters["event_view_event_id_column"] = data_input_node.parameters.id_column  # type: ignore

        return parameters

    async def _prepare_view_graph_node_name_to_replacement_node(
        self, feature: FeatureModel, data_cleaning_operations: Optional[List[DataCleaningOperation]]
    ) -> dict[str, Node]:
        """
        Prepare view graph node name to replacement node mapping by using the provided data cleaning and
        the view graph node metadata.

        Parameters
        ----------
        feature: FeatureModel
            Feature model
        data_cleaning_operations: Optional[List[DataCleaningOperation]]
            Data cleaning operations

        Returns
        -------
        dict[str, Node]

        Raises
        ------
        GraphInconsistencyError
            If the graph has unexpected structure
        """
        if not data_cleaning_operations:
            return {}

        node_name_to_replacement_node: dict[str, Node] = {}
        data_name_to_column_cleaning_operations: dict[str, List[ColumnCleaningOperation]] = {
            data_clean_op.data_name: data_clean_op.column_cleaning_operations
            for data_clean_op in data_cleaning_operations
        }
        # view node name to (view graph node, view columns_info & data input node)
        view_node_name_to_data_info: dict[str, Tuple[Node, List[ColumnInfo], InputNode]] = {}

        # since the graph node is topologically sorted, input to the view graph node will always be
        # processed before the view graph node itself
        for graph_node in feature.graph.iterate_sorted_graph_nodes(
            graph_node_types=GraphNodeType.view_graph_node_types()
        ):
            # the first graph node of view graph node is always the data input node
            input_node_names = feature.graph.get_input_node_names(node=graph_node)
            view_graph_input_node = feature.graph.get_node_by_name(node_name=input_node_names[0])

            if not isinstance(view_graph_input_node, InputNode):
                raise GraphInconsistencyError(
                    "First input node of view graph node is not an input node."
                )

            # additional parameters used to construct view graph node
            create_view_graph_node_kwargs: dict[
                str, Any
            ] = self._get_create_view_graph_node_keyword_parameters(
                graph_node=graph_node,
                input_node_names=input_node_names,
                view_node_name_to_data_info=view_node_name_to_data_info,
            )

            # get the data document based on the data ID in input node
            data_id = view_graph_input_node.parameters.id
            assert data_id is not None
            data_document = await self.data_service.get_document(document_id=data_id)
            assert data_document.name is not None
            parameters_metadata = graph_node.parameters.metadata  # type: ignore
            assert isinstance(parameters_metadata, ViewMetadata)

            # check whether the data document has column cleaning operations
            # use the column cleaning operations to create a new view graph node
            column_clean_ops = data_name_to_column_cleaning_operations.get(data_document.name)
            if column_clean_ops:
                parameters_metadata = parameters_metadata.clone(
                    view_mode=parameters_metadata.view_mode,
                    column_cleaning_operations=column_clean_ops,
                )

            new_view_graph_node, view_columns_info = data_document.create_view_graph_node(
                input_node=view_graph_input_node,
                metadata=parameters_metadata,
                **create_view_graph_node_kwargs,
            )
            view_node_name_to_data_info[graph_node.name] = (
                new_view_graph_node,
                view_columns_info,
                view_graph_input_node,
            )

            # check whether there is any changes on the new view graph node
            if new_view_graph_node != graph_node:
                node_name_to_replacement_node[graph_node.name] = new_view_graph_node

        return node_name_to_replacement_node

    @staticmethod
    def _create_new_feature_version_from(
        feature: FeatureModel, node_name_to_replacement_node: dict[str, Node]
    ) -> Optional[FeatureModel]:
        """
        Create a new feature version from the provided feature model and node name to replacement node mapping.

        Parameters
        ----------
        feature: FeatureModel
            Feature model
        node_name_to_replacement_node: dict[str, Node]
            Node name to replacement node mapping

        Returns
        -------
        Optional[FeatureModel]
        """
        if node_name_to_replacement_node:
            graph, node_name_map = feature.graph.reconstruct(
                node_name_to_replacement_node=node_name_to_replacement_node,
                regenerate_groupby_hash=True,
            )
            node_name = node_name_map[feature.node_name]

            # prune the graph to remove unnecessary nodes
            pruned_graph, node_name_map = QueryGraph(**graph.dict(by_alias=True)).prune(
                target_node=graph.get_node_by_name(node_name), aggressive=True
            )
            pruned_node_name = node_name_map[node_name]

            # only return a new feature version if the graph is changed
            reference_hash_before = feature.graph.node_name_to_ref[feature.node_name]
            reference_hash_after = pruned_graph.node_name_to_ref[pruned_node_name]
            if reference_hash_before != reference_hash_after:
                return FeatureModel(
                    **{
                        **feature.dict(),
                        "graph": pruned_graph,
                        "node_name": pruned_node_name,
                        "_id": ObjectId(),
                    }
                )
        return None

    async def _create_new_feature_version(
        self,
        feature: FeatureModel,
        feature_job_setting: Optional[FeatureJobSetting],
        data_cleaning_operations: Optional[List[DataCleaningOperation]],
    ) -> FeatureModel:
        node_name_to_replacement_node: dict[str, Node] = {}

        # prepare group by node replacement
        group_by_node_replacement = self._prepare_group_by_node_name_to_replacement_node(
            feature=feature, feature_job_setting=feature_job_setting
        )
        node_name_to_replacement_node.update(group_by_node_replacement)

        # prepare view graph node replacement
        view_graph_node_replacement = await self._prepare_view_graph_node_name_to_replacement_node(
            feature=feature, data_cleaning_operations=data_cleaning_operations
        )
        node_name_to_replacement_node.update(view_graph_node_replacement)

        # create a new feature version if the new feature graph is different from the source feature graph
        new_feature_version = self._create_new_feature_version_from(
            feature=feature, node_name_to_replacement_node=node_name_to_replacement_node
        )
        if new_feature_version:
            return new_feature_version

        # no new feature is created, raise the error message accordingly
        if feature_job_setting or data_cleaning_operations:
            actions = []
            if feature_job_setting:
                actions.append("feature job setting")
            if data_cleaning_operations:
                actions.append("data cleaning operation(s)")

            actions_str = " and ".join(actions).capitalize()
            do_str = "do" if len(actions) > 1 else "does"
            error_message = (
                f"{actions_str} {do_str} not result a new feature version. "
                "This is because the new feature version is the same as the source feature."
            )
            raise DocumentError(error_message)
        raise DocumentError("No change detected on the new feature version.")

    async def create_new_feature_version(
        self,
        data: FeatureNewVersionCreate,
    ) -> FeatureModel:
        """
        Create new feature version based on given source feature

        Parameters
        ----------
        data: FeatureNewVersionCreate
            Version creation payload

        Returns
        -------
        FeatureModel
        """
        feature = await self.feature_service.get_document(document_id=data.source_feature_id)
        new_feature = await self._create_new_feature_version(
            feature, data.feature_job_setting, data.data_cleaning_operations
        )
        return await self.feature_service.create_document(
            data=FeatureCreate(**new_feature.dict(by_alias=True))
        )

    async def _create_new_feature_list_version(
        self,
        feature_list: FeatureListModel,
        feature_namespaces: list[dict[str, Any]],
        data: FeatureListNewVersionCreate,
    ) -> FeatureListModel:
        feat_name_to_default_id_map = {
            feat_namespace["name"]: feat_namespace["default_feature_id"]
            for feat_namespace in feature_namespaces
        }
        if data.mode == FeatureListNewVersionMode.AUTO:
            # for auto mode, use default feature id for all the features within the feature list
            features = []
            for feature_id in feat_name_to_default_id_map.values():
                features.append(await self.feature_service.get_document(document_id=feature_id))
        else:
            if not data.features:
                raise DocumentError("Feature info is missing.")

            feature_id_to_name_map = {
                feat_id: feature_namespace["name"]
                for feature_namespace in feature_namespaces
                for feat_id in feature_namespace["feature_ids"]
            }
            specified_feature_map = {
                feat_info.name: feat_info.version for feat_info in data.features
            }
            features = []
            for feat_id in feature_list.feature_ids:
                feat_name = feature_id_to_name_map[feat_id]
                if feat_name in specified_feature_map:
                    version = specified_feature_map.pop(feat_name)
                    feature = await self.feature_service.get_document_by_name_and_version(
                        name=feat_name, version=version
                    )
                    features.append(feature)
                else:
                    # for semi-auto mode, use default feature id for non-specified features
                    # for manual mode, use the original feature id of the feature list for non-specified features
                    if data.mode == FeatureListNewVersionMode.SEMI_AUTO:
                        feat_id = feat_name_to_default_id_map[feat_name]
                    features.append(await self.feature_service.get_document(document_id=feat_id))

            if specified_feature_map:
                names = [f'"{name}"' for name in specified_feature_map.keys()]
                raise DocumentError(
                    f"Features ({', '.join(names)}) are not in the original FeatureList"
                )

        feature_ids = [feat.id for feat in features]
        if set(feature_list.feature_ids) == set(feature_ids):
            raise DocumentError("No change detected on the new feature list version.")

        return FeatureListModel(
            **{
                **feature_list.dict(),
                "_id": ObjectId(),
                "feature_ids": feature_ids,
                "features": features,
            }
        )

    async def create_new_feature_list_version(
        self,
        data: FeatureListNewVersionCreate,
    ) -> FeatureListModel:
        """
        Create new feature list version based on given source feature list & new version mode

        Parameters
        ----------
        data: FeatureListNewVersionCreate
            Version creation payload

        Returns
        -------
        FeatureListModel
        """
        feature_list = await self.feature_list_service.get_document(
            document_id=data.source_feature_list_id
        )
        feature_list_namespace = await self.feature_list_namespace_service.get_document(
            document_id=feature_list.feature_list_namespace_id,
        )
        feature_namespaces = await self.feature_namespace_service.list_documents(
            query_filter={"_id": {"$in": feature_list_namespace.feature_namespace_ids}},
            page_size=0,
        )
        new_feature_list = await self._create_new_feature_list_version(
            feature_list, feature_namespaces["data"], data
        )
        return await self.feature_list_service.create_document(
            data=FeatureListCreate(**new_feature_list.dict(by_alias=True)),
        )
