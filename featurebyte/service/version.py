"""
VersionService class
"""

from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId

from featurebyte.enum import TableDataType
from featurebyte.exception import (
    DocumentError,
    NoChangesInFeatureVersionError,
    NoFeatureJobSettingInSourceError,
)
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.feature_job_setting import (
    CronFeatureJobSetting,
    FeatureJobSetting,
    FeatureJobSettingUnion,
    TableFeatureJobSetting,
)
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.cleaning_operation import TableCleaningOperation
from featurebyte.schema.feature import FeatureNewVersionCreate, FeatureServiceCreate
from featurebyte.schema.feature_list import FeatureListNewVersionCreate, FeatureListServiceCreate
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.table import TableService
from featurebyte.service.view_construction import ViewConstructionService


class VersionService:
    """
    VersionService class is responsible for creating new feature version
    """

    def __init__(
        self,
        table_service: TableService,
        feature_service: FeatureService,
        feature_namespace_service: FeatureNamespaceService,
        feature_list_service: FeatureListService,
        feature_list_namespace_service: FeatureListNamespaceService,
        view_construction_service: ViewConstructionService,
    ):
        self.table_service = table_service
        self.feature_service = feature_service
        self.feature_namespace_service = feature_namespace_service
        self.feature_list_service = feature_list_service
        self.feature_list_namespace_service = feature_list_namespace_service
        self.view_construction_service = view_construction_service

    async def _prepare_group_by_node_name_to_replacement_node(
        self,
        feature: FeatureModel,
        table_feature_job_settings: Optional[list[TableFeatureJobSetting]],
        use_source_settings: bool,
    ) -> dict[str, Node]:
        """
        Prepare group by node name to replacement node mapping by using the provided feature job setting
        to construct new group by node. If the new group by node is the same as the existing one, then
        the mapping will be empty.

        Parameters
        ----------
        feature: FeatureModel
            Feature model
        table_feature_job_settings: Optional[list[TableFeatureJobSetting]]
            Feature job setting
        use_source_settings: bool
            Whether to use source table's default feature job setting

        Returns
        -------
        dict[str, Node]

        Raises
        ------
        NoFeatureJobSettingInSourceError
            If the source table does not have a default feature job setting
        """
        node_name_to_replacement_node: dict[str, Node] = {}
        table_feature_job_settings = table_feature_job_settings or []
        if table_feature_job_settings or use_source_settings:
            table_name_to_feature_job_setting: dict[str, FeatureJobSettingUnion] = {}
            for table_feature_job_setting in table_feature_job_settings:
                table_name_to_feature_job_setting[table_feature_job_setting.table_name] = (
                    table_feature_job_setting.feature_job_setting
                )

            table_id_to_table: dict[ObjectId, ProxyTableModel] = {
                doc["_id"]: ProxyTableModel(**doc)  # type: ignore
                async for doc in self.table_service.list_documents_as_dict_iterator(
                    query_filter={"_id": {"$in": feature.table_ids}}
                )
            }
            for (
                agg_node,
                table_id,
            ) in feature.graph.iterate_group_by_node_and_table_id_pairs(target_node=feature.node):
                # prepare feature job setting
                assert table_id is not None, "Table ID should not be None."
                table = table_id_to_table[table_id]
                feature_job_setting: Optional[FeatureJobSettingUnion] = None
                if use_source_settings:
                    # use the event table source's default feature job setting if table is event table
                    # otherwise, do not create a replacement node for the group by node
                    if table.type in TableDataType.with_default_feature_job_setting():
                        assert hasattr(
                            table, "default_feature_job_setting"
                        ), "Table should have default feature job setting attribute."
                        feature_job_setting = table.default_feature_job_setting
                        if not feature_job_setting:
                            raise NoFeatureJobSettingInSourceError(
                                f"No feature job setting found in source id {table_id}"
                            )
                        # Replacing CronFeatureJobSetting with unconstrained FeatureJobSetting is
                        # not supported
                        if isinstance(
                            agg_node.parameters.feature_job_setting, CronFeatureJobSetting
                        ) and isinstance(feature_job_setting, FeatureJobSetting):
                            feature_job_setting = None
                else:
                    # use the provided feature job setting
                    assert table.name is not None, "Table name should not be None."
                    feature_job_setting = table_name_to_feature_job_setting.get(table.name)

                if feature_job_setting:
                    # input node will be used when we need to support updating specific
                    # aggregation node given table ID
                    parameters = agg_node.parameters.model_dump()
                    parameters["feature_job_setting"] = feature_job_setting.model_dump()
                    if agg_node.parameters.model_dump() != parameters:
                        node_class = type(agg_node)
                        node_name_to_replacement_node[agg_node.name] = node_class(**{
                            **agg_node.model_dump(),
                            "parameters": parameters,
                        })

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
        # reconstruct view graph node to remove unused column cleaning operations
        graph, node_name_map = feature.graph.reconstruct(
            node_name_to_replacement_node=node_name_to_replacement_node,
            regenerate_groupby_hash=True,
        )
        node_name = node_name_map[feature.node_name]

        # prune the graph to remove unused nodes
        pruned_graph, node_name_map = QueryGraph(**graph.model_dump(by_alias=True)).prune(
            target_node=graph.get_node_by_name(node_name),
        )
        pruned_node_name = node_name_map[node_name]

        # only return a new feature version if the graph is changed
        reference_hash_before = feature.graph.node_name_to_ref[feature.node_name]
        reference_hash_after = graph.node_name_to_ref[node_name]
        if reference_hash_before != reference_hash_after:
            # only include fields that are required for creating a new feature version,
            # other attributes will be re-generated when the new feature version is constructed
            include_fields = {"name", "tabular_source", "feature_namespace_id"}
            return FeatureModel(**{
                **feature.model_dump(include=include_fields),
                "graph": pruned_graph,
                "node_name": pruned_node_name,
                "_id": ObjectId(),
            })
        return None

    async def _create_new_feature_version(
        self,
        feature: FeatureModel,
        table_feature_job_settings: Optional[list[TableFeatureJobSetting]],
        table_cleaning_operations: Optional[list[TableCleaningOperation]],
        use_source_settings: bool,
    ) -> FeatureModel:
        node_name_to_replacement_node: dict[str, Node] = {}

        # prepare group by node replacement
        group_by_node_replacement = await self._prepare_group_by_node_name_to_replacement_node(
            feature=feature,
            table_feature_job_settings=table_feature_job_settings,
            use_source_settings=use_source_settings,
        )
        node_name_to_replacement_node.update(group_by_node_replacement)

        # prepare view graph node replacement
        if table_cleaning_operations or use_source_settings:
            view_node_name_replacement = (
                await self.view_construction_service.prepare_view_node_name_to_replacement_node(
                    query_graph=feature.graph,
                    target_node=feature.node,
                    table_cleaning_operations=table_cleaning_operations or [],
                    use_source_settings=use_source_settings,
                )
            )
            node_name_to_replacement_node.update(view_node_name_replacement)

        # create a new feature version if the new feature graph is different from the source feature graph
        new_feature_version = self._create_new_feature_version_from(
            feature=feature, node_name_to_replacement_node=node_name_to_replacement_node
        )
        if new_feature_version:
            return new_feature_version

        # no new feature is created, raise the error message accordingly
        if table_feature_job_settings or table_cleaning_operations:
            actions = []
            if table_feature_job_settings:
                actions.append("feature job setting")
            if table_cleaning_operations:
                actions.append("table cleaning operation(s)")

            actions_str = " and ".join(actions).capitalize()
            do_str = "do" if len(actions) > 1 else "does"
            error_message = (
                f"{actions_str} {do_str} not result a new feature version. "
                "This is because the new feature version is the same as the source feature."
            )
            raise NoChangesInFeatureVersionError(error_message)
        raise NoChangesInFeatureVersionError("No change detected on the new feature version.")

    async def create_new_feature_version(
        self,
        data: FeatureNewVersionCreate,
        to_save: bool = True,
    ) -> FeatureModel:
        """
        Create new feature version based on given source feature
        (newly created feature will be saved to the persistent)

        Parameters
        ----------
        data: FeatureNewVersionCreate
            Version creation payload
        to_save: bool
            Whether to save the newly created feature to the persistent

        Returns
        -------
        FeatureModel
        """
        feature = await self.feature_service.get_document(document_id=data.source_feature_id)
        new_feature = await self._create_new_feature_version(
            feature,
            data.table_feature_job_settings,
            data.table_cleaning_operations,
            use_source_settings=False,
        )
        if to_save:
            return await self.feature_service.create_document(
                data=FeatureServiceCreate(**new_feature.model_dump(by_alias=True))
            )
        return new_feature

    async def create_new_feature_version_using_source_settings(
        self, document_id: ObjectId
    ) -> FeatureModel:
        """
        Create new feature version based on feature job settings & cleaning operation from the source table
        (newly created feature won't be saved to the persistent)

        Parameters
        ----------
        document_id: ObjectId
            Feature id used to create new version

        Returns
        -------
        FeatureModel
        """
        feature = await self.feature_service.get_document(document_id=document_id)
        new_feature = await self._create_new_feature_version(
            feature,
            table_feature_job_settings=None,
            table_cleaning_operations=None,
            use_source_settings=True,
        )
        return new_feature

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
        feature_id_to_name_map = {
            feat_id: feature_namespace["name"]
            for feature_namespace in feature_namespaces
            for feat_id in feature_namespace["feature_ids"]
        }
        specified_feature_map = {feat_info.name: feat_info.version for feat_info in data.features}
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
                # use default feature id for non-specified features
                feat_id = feat_name_to_default_id_map[feat_name]
                features.append(await self.feature_service.get_document(document_id=feat_id))

        if specified_feature_map:
            names = [f'"{name}"' for name in specified_feature_map.keys()]
            raise DocumentError(
                f"Features ({', '.join(names)}) are not in the original FeatureList"
            )

        feature_ids = [feat.id for feat in features]
        if set(feature_list.feature_ids) == set(feature_ids):
            if data.allow_unchanged_feature_list_version:
                return feature_list

            raise DocumentError("No change detected on the new feature list version.")

        # extract features metadata
        features_metadata = await self.feature_list_service.extract_features_metadata(
            feature_ids=feature_ids
        )

        return FeatureListModel(**{
            **feature_list.model_dump(),
            "_id": ObjectId(),
            "feature_ids": feature_ids,
            "features": features,
            "features_metadata": features_metadata,
        })

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
        feature_namespaces = await self.feature_namespace_service.list_documents_as_dict(
            query_filter={"_id": {"$in": feature_list_namespace.feature_namespace_ids}},
            page_size=0,
        )
        new_feature_list = await self._create_new_feature_list_version(
            feature_list, feature_namespaces["data"], data
        )
        if new_feature_list.id == feature_list.id:
            return feature_list

        return await self.feature_list_service.create_document(
            data=FeatureListServiceCreate(**new_feature_list.model_dump(by_alias=True)),
        )
