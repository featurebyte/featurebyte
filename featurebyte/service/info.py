"""
InfoService class
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Type, TypeVar

from bson.objectid import ObjectId

from featurebyte import DataCleaningOperation
from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import DataModel
from featurebyte.models.tabular_data import TabularDataModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure
from featurebyte.schema.feature import FeatureBriefInfoList
from featurebyte.schema.info import (
    DataBriefInfoList,
    DimensionDataInfo,
    EntityBriefInfoList,
    EntityInfo,
    EventDataInfo,
    FeatureInfo,
    FeatureJobSettingAnalysisInfo,
    FeatureListBriefInfoList,
    FeatureListInfo,
    FeatureListNamespaceInfo,
    FeatureNamespaceInfo,
    FeatureStoreInfo,
    ItemDataInfo,
    SCDDataInfo,
    WorkspaceInfo,
)
from featurebyte.schema.relationship_info import RelationshipInfoInfo
from featurebyte.schema.semantic import SemanticList
from featurebyte.schema.tabular_data import TabularDataList
from featurebyte.service.base_document import BaseDocumentService, DocumentUpdateSchema
from featurebyte.service.base_service import BaseService
from featurebyte.service.dimension_data import DimensionDataService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.item_data import ItemDataService
from featurebyte.service.mixin import Document, DocumentCreateSchema
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.scd_data import SCDDataService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.tabular_data import DataService
from featurebyte.service.user_service import UserService
from featurebyte.service.workspace import WorkspaceService

ObjectT = TypeVar("ObjectT")


class InfoService(BaseService):
    """
    InfoService class is responsible for rendering the info of a specific api object.
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self, user: Any, persistent: Persistent, workspace_id: ObjectId):
        super().__init__(user, persistent, workspace_id)
        self.data_service = DataService(user=user, persistent=persistent, workspace_id=workspace_id)
        self.event_data_service = EventDataService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.item_data_service = ItemDataService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.dimension_data_service = DimensionDataService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.scd_data_service = SCDDataService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.semantic_service = SemanticService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.feature_store_service = FeatureStoreService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.entity_service = EntityService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
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
        self.feature_job_setting_analysis_service = FeatureJobSettingAnalysisService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.workspace_service = WorkspaceService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.relationship_info_service = RelationshipInfoService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        self.user_service = UserService(user=user, persistent=persistent, workspace_id=workspace_id)

    @staticmethod
    async def _get_list_object(
        service: BaseDocumentService[Document, DocumentCreateSchema, DocumentUpdateSchema],
        document_ids: list[PydanticObjectId],
        list_object_class: Type[ObjectT],
    ) -> ObjectT:
        """
        Retrieve object through list route & deserialize the records

        Parameters
        ----------
        service: BaseDocumentService
            Service
        document_ids: list[ObjectId]
            List of document IDs
        list_object_class: Type[ObjectT]
            List object class

        Returns
        -------
        ObjectT
        """
        res = await service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": document_ids}}
        )
        return list_object_class(**{**res, "page_size": 1})

    async def get_feature_store_info(
        self, document_id: ObjectId, verbose: bool
    ) -> FeatureStoreInfo:
        """
        Get feature store info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        FeatureStoreInfo
        """
        _ = verbose
        feature_store = await self.feature_store_service.get_document(document_id=document_id)
        return FeatureStoreInfo(
            name=feature_store.name,
            created_at=feature_store.created_at,
            updated_at=feature_store.updated_at,
            source=feature_store.type,
            database_details=feature_store.details,
        )

    async def get_entity_info(self, document_id: ObjectId, verbose: bool) -> EntityInfo:
        """
        Get entity info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        EntityInfo
        """
        _ = verbose
        entity = await self.entity_service.get_document(document_id=document_id)

        # get workspace info
        workspace = await self.workspace_service.get_document(entity.workspace_id)

        return EntityInfo(
            name=entity.name,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
            serving_names=entity.serving_names,
            workspace_name=workspace.name,
        )

    async def _get_data_info(self, data_document: DataModel, verbose: bool) -> Dict[str, Any]:
        """
        Get data info

        Parameters
        ----------
        data_document: DataModel
            Data document (could be event data, SCD data, item data, dimension data, etc)
        verbose: bool
            Verbose or not

        Returns
        -------
        Dict[str, Any]
        """
        entities = await self.entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": data_document.entity_ids}}
        )
        semantics = await self.semantic_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": data_document.semantic_ids}}
        )
        columns_info = None
        if verbose:
            columns_info = []
            entity_map = {ObjectId(entity["_id"]): entity["name"] for entity in entities["data"]}
            semantic_map = {
                ObjectId(semantic["_id"]): semantic["name"] for semantic in semantics["data"]
            }
            for column_info in data_document.columns_info:
                columns_info.append(
                    {
                        **column_info.dict(),
                        "entity": entity_map.get(column_info.entity_id),  # type: ignore[arg-type]
                        "semantic": semantic_map.get(column_info.semantic_id),  # type: ignore[arg-type]
                        "critical_data_info": column_info.critical_data_info,
                    }
                )

        # get workspace info
        workspace = await self.workspace_service.get_document(data_document.workspace_id)
        for entity in entities["data"]:
            assert entity["workspace_id"] == workspace.id
            entity["workspace_name"] = workspace.name

        return {
            "name": data_document.name,
            "created_at": data_document.created_at,
            "updated_at": data_document.updated_at,
            "record_creation_date_column": data_document.record_creation_date_column,
            "table_details": data_document.tabular_source.table_details,
            "status": data_document.status,
            "entities": EntityBriefInfoList.from_paginated_data(entities),
            "semantics": [semantic["name"] for semantic in semantics["data"]],
            "column_count": len(data_document.columns_info),
            "columns_info": columns_info,
            "workspace_name": workspace.name,
        }

    async def get_relationship_info_info(self, document_id: ObjectId) -> RelationshipInfoInfo:
        """
        Get relationship info info

        Parameters
        ----------
        document_id: ObjectId
            Document ID

        Returns
        -------
        RelationshipInfoInfo
        """
        relationship_info = await self.relationship_info_service.get_document(
            document_id=document_id
        )
        data_info = await self.data_service.get_document(
            document_id=relationship_info.primary_data_source_id
        )
        updated_user_name = self.user_service.get_user_name_for_id(relationship_info.updated_by)
        primary_entity = await self.entity_service.get_document(
            document_id=relationship_info.primary_entity_id
        )
        related_entity = await self.entity_service.get_document(
            document_id=relationship_info.related_entity_id
        )
        return RelationshipInfoInfo(
            name=relationship_info.name,
            created_at=relationship_info.created_at,
            updated_at=relationship_info.updated_at,
            relationship_type=relationship_info.relationship_type,
            data_source_name=data_info.name,
            primary_entity_name=primary_entity.name,
            related_entity_name=related_entity.name,
            updated_by=updated_user_name,
        )

    async def get_event_data_info(self, document_id: ObjectId, verbose: bool) -> EventDataInfo:
        """
        Get event data info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        EventDataInfo
        """
        event_data = await self.event_data_service.get_document(document_id=document_id)
        data_dict = await self._get_data_info(data_document=event_data, verbose=verbose)
        return EventDataInfo(
            **data_dict,
            event_id_column=event_data.event_id_column,
            event_timestamp_column=event_data.event_timestamp_column,
            default_feature_job_setting=event_data.default_feature_job_setting,
        )

    async def get_item_data_info(self, document_id: ObjectId, verbose: bool) -> ItemDataInfo:
        """
        Get item data info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        ItemDataInfo
        """
        item_data = await self.item_data_service.get_document(document_id=document_id)
        data_dict = await self._get_data_info(data_document=item_data, verbose=verbose)
        event_data = await self.event_data_service.get_document(document_id=item_data.event_data_id)
        return ItemDataInfo(
            **data_dict,
            event_id_column=item_data.event_id_column,
            item_id_column=item_data.item_id_column,
            event_data_name=event_data.name,
        )

    async def get_dimension_data_info(
        self, document_id: ObjectId, verbose: bool
    ) -> DimensionDataInfo:
        """
        Get dimension data info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        DimensionDataInfo
        """
        dimension_data = await self.dimension_data_service.get_document(document_id=document_id)
        data_dict = await self._get_data_info(data_document=dimension_data, verbose=verbose)
        return DimensionDataInfo(
            **data_dict,
            dimension_id_column=dimension_data.dimension_id_column,
        )

    async def get_scd_data_info(self, document_id: ObjectId, verbose: bool) -> SCDDataInfo:
        """
        Get Slow Changing Dimension data info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        SCDDataInfo
        """
        scd_data = await self.scd_data_service.get_document(document_id=document_id)
        data_dict = await self._get_data_info(data_document=scd_data, verbose=verbose)
        return SCDDataInfo(
            **data_dict,
            natural_key_column=scd_data.natural_key_column,
            effective_timestamp_column=scd_data.effective_timestamp_column,
            surrogate_key_column=scd_data.surrogate_key_column,
            end_timestamp_column=scd_data.end_timestamp_column,
            current_flag_column=scd_data.current_flag_column,
        )

    @staticmethod
    def _get_main_data(tabular_data_list: list[TabularDataModel]) -> TabularDataModel:
        """
        Get the main data from the list of tabular data

        Parameters
        ----------
        tabular_data_list: list[TabularDataModel]
            List of tabular data model

        Returns
        -------
        TabularDataModel
        """
        data_priority_map = {}
        for tabular_data in tabular_data_list:
            if tabular_data.type == TableDataType.ITEM_DATA:
                data_priority_map[3] = tabular_data
            elif tabular_data.type == TableDataType.EVENT_DATA:
                data_priority_map[2] = tabular_data
            elif tabular_data.entity_ids:
                data_priority_map[1] = tabular_data
            else:
                data_priority_map[0] = tabular_data
        return data_priority_map[max(data_priority_map)]

    async def _extract_feature_metadata(self, op_struct: GroupOperationStructure) -> dict[str, Any]:
        # retrieve related tabular data & semantic
        tabular_data_list = await self._get_list_object(
            self.data_service, op_struct.tabular_data_ids, TabularDataList
        )
        semantic_list = await self._get_list_object(
            self.semantic_service, tabular_data_list.semantic_ids, SemanticList
        )

        # prepare column mapping
        column_map: dict[tuple[Optional[ObjectId], str], Any] = {}
        semantic_map = {semantic.id: semantic.name for semantic in semantic_list.data}
        for tabular_data in tabular_data_list.data:
            for column in tabular_data.columns_info:
                column_map[(tabular_data.id, column.name)] = {
                    "data_name": tabular_data.name,
                    "semantic": semantic_map.get(column.semantic_id),  # type: ignore
                }

        # construct feature metadata
        source_columns = {}
        reference_map: dict[Any, str] = {}
        for idx, src_col in enumerate(op_struct.source_columns):
            column_metadata = column_map[(src_col.tabular_data_id, src_col.name)]
            reference_map[src_col] = f"Input{idx}"
            source_columns[reference_map[src_col]] = {
                "data": column_metadata["data_name"],
                "column_name": src_col.name,
                "semantic": column_metadata["semantic"],
            }

        derived_columns = {}
        for idx, drv_col in enumerate(op_struct.derived_columns):
            columns = [reference_map[col] for col in drv_col.columns]
            reference_map[drv_col] = f"X{idx}"
            derived_columns[reference_map[drv_col]] = {
                "name": drv_col.name,
                "inputs": columns,
                "transforms": drv_col.transforms,
            }

        aggregation_columns = {}
        for idx, agg_col in enumerate(op_struct.aggregations):
            reference_map[agg_col] = f"F{idx}"
            aggregation_columns[reference_map[agg_col]] = {
                "name": agg_col.name,
                "column": reference_map.get(
                    agg_col.column, None
                ),  # for count aggregation, column is None
                "function": agg_col.method,
                "keys": agg_col.keys,
                "window": agg_col.window,
                "category": agg_col.category,
                "filter": agg_col.filter,
            }

        post_aggregation = None
        if op_struct.post_aggregation:
            post_aggregation = {
                "name": op_struct.post_aggregation.name,
                "inputs": [reference_map[col] for col in op_struct.post_aggregation.columns],
                "transforms": op_struct.post_aggregation.transforms,
            }

        main_data = self._get_main_data(tabular_data_list.data)
        return {
            "main_data": {"name": main_data.name, "data_type": main_data.type, "id": main_data.id},
            "input_columns": source_columns,
            "derived_columns": derived_columns,
            "aggregations": aggregation_columns,
            "post_aggregation": post_aggregation,
        }

    async def get_feature_info(self, document_id: ObjectId, verbose: bool) -> FeatureInfo:
        """
        Get feature info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        FeatureInfo
        """
        feature = await self.feature_service.get_document(document_id=document_id)
        data_id_to_name = {
            doc["_id"]: doc["name"]
            async for doc in self.data_service.list_documents_iterator(
                query_filter={"_id": {"$in": feature.tabular_data_ids}}
            )
        }

        data_cleaning_operations: list[DataCleaningOperation] = []
        for view_graph_node in feature.graph.iterate_sorted_graph_nodes(
            graph_node_types=GraphNodeType.view_graph_node_types()
        ):
            view_metadata = view_graph_node.parameters.metadata
            if view_metadata.column_cleaning_operations:
                data_cleaning_operations.append(
                    DataCleaningOperation(
                        data_name=data_id_to_name[view_metadata.data_id],
                        column_cleaning_operations=view_metadata.column_cleaning_operations,
                    )
                )

        namespace_info = await self.get_feature_namespace_info(
            document_id=feature.feature_namespace_id,
            verbose=verbose,
        )
        default_feature = await self.feature_service.get_document(
            document_id=namespace_info.default_feature_id
        )
        versions_info = None
        if verbose:
            namespace = await self.feature_namespace_service.get_document(
                document_id=feature.feature_namespace_id
            )
            versions_info = FeatureBriefInfoList.from_paginated_data(
                await self.feature_service.list_documents(
                    page=1,
                    page_size=0,
                    query_filter={"_id": {"$in": namespace.feature_ids}},
                )
            )

        op_struct = feature.extract_operation_structure()
        if op_struct.tabular_data_ids:
            metadata = await self._extract_feature_metadata(op_struct=op_struct)
        else:
            # DEV-556: handle the case before tracking this field in the input node
            metadata = None

        return FeatureInfo(
            **namespace_info.dict(),
            version={"this": feature.version.to_str(), "default": default_feature.version.to_str()},
            readiness={"this": feature.readiness, "default": default_feature.readiness},
            versions_info=versions_info,
            metadata=metadata,
            data_cleaning_operations=data_cleaning_operations,
        )

    async def get_feature_namespace_info(
        self, document_id: ObjectId, verbose: bool
    ) -> FeatureNamespaceInfo:
        """
        Get feature namespace info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        FeatureNamespaceInfo
        """
        _ = verbose
        namespace = await self.feature_namespace_service.get_document(document_id=document_id)
        entities = await self.entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.entity_ids}}
        )

        tabular_data = await self.data_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.tabular_data_ids}}
        )

        # get workspace info
        workspace = await self.workspace_service.get_document(namespace.workspace_id)
        for entity in entities["data"]:
            assert entity["workspace_id"] == workspace.id
            entity["workspace_name"] = workspace.name
        for data in tabular_data["data"]:
            assert data["workspace_id"] == workspace.id
            data["workspace_name"] = workspace.name

        return FeatureNamespaceInfo(
            name=namespace.name,
            created_at=namespace.created_at,
            updated_at=namespace.updated_at,
            entities=EntityBriefInfoList.from_paginated_data(entities),
            tabular_data=DataBriefInfoList.from_paginated_data(tabular_data),
            default_version_mode=namespace.default_version_mode,
            default_feature_id=namespace.default_feature_id,
            dtype=namespace.dtype,
            version_count=len(namespace.feature_ids),
            workspace_name=workspace.name,
        )

    async def get_feature_list_info(self, document_id: ObjectId, verbose: bool) -> FeatureListInfo:
        """
        Get feature list info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        FeatureListInfo
        """
        feature_list = await self.feature_list_service.get_document(document_id=document_id)
        namespace_info = await self.get_feature_list_namespace_info(
            document_id=feature_list.feature_list_namespace_id,
            verbose=verbose,
        )
        default_feature_list = await self.feature_list_service.get_document(
            document_id=namespace_info.default_feature_list_id
        )
        versions_info = None
        if verbose:
            namespace = await self.feature_list_namespace_service.get_document(
                document_id=feature_list.feature_list_namespace_id
            )
            versions_info = FeatureListBriefInfoList.from_paginated_data(
                await self.feature_list_service.list_documents(
                    page=1,
                    page_size=0,
                    query_filter={"_id": {"$in": namespace.feature_list_ids}},
                )
            )

        return FeatureListInfo(
            **namespace_info.dict(),
            version={
                "this": feature_list.version.to_str() if feature_list.version else None,
                "default": default_feature_list.version.to_str()
                if default_feature_list.version
                else None,
            },
            production_ready_fraction={
                "this": feature_list.readiness_distribution.derive_production_ready_fraction(),
                "default": default_feature_list.readiness_distribution.derive_production_ready_fraction(),
            },
            versions_info=versions_info,
            deployed=feature_list.deployed,
            serving_endpoint=f"/feature_list/{feature_list.id}/online_features"
            if feature_list.deployed
            else None,
        )

    async def get_feature_list_namespace_info(
        self, document_id: ObjectId, verbose: bool
    ) -> FeatureListNamespaceInfo:
        """
        Get feature list namespace info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        FeatureListNamespaceInfo
        """
        _ = verbose
        namespace = await self.feature_list_namespace_service.get_document(document_id=document_id)
        entities = await self.entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.entity_ids}}
        )

        tabular_data = await self.data_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.tabular_data_ids}}
        )

        # get workspace info
        workspace = await self.workspace_service.get_document(namespace.workspace_id)
        for entity in entities["data"]:
            assert entity["workspace_id"] == workspace.id
            entity["workspace_name"] = workspace.name
        for data in tabular_data["data"]:
            assert data["workspace_id"] == workspace.id
            data["workspace_name"] = workspace.name

        return FeatureListNamespaceInfo(
            name=namespace.name,
            created_at=namespace.created_at,
            updated_at=namespace.updated_at,
            entities=EntityBriefInfoList.from_paginated_data(entities),
            tabular_data=DataBriefInfoList.from_paginated_data(tabular_data),
            default_version_mode=namespace.default_version_mode,
            default_feature_list_id=namespace.default_feature_list_id,
            dtype_distribution=namespace.dtype_distribution,
            version_count=len(namespace.feature_list_ids),
            feature_count=len(namespace.feature_namespace_ids),
            status=namespace.status,
            workspace_name=workspace.name,
        )

    async def get_feature_job_setting_analysis_info(
        self, document_id: ObjectId, verbose: bool
    ) -> FeatureJobSettingAnalysisInfo:
        """
        Get item data info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        FeatureJobSettingAnalysisInfo
        """
        _ = verbose
        feature_job_setting_analysis = await self.feature_job_setting_analysis_service.get_document(
            document_id=document_id
        )
        recommended_setting = (
            feature_job_setting_analysis.analysis_result.recommended_feature_job_setting
        )
        event_data = await self.event_data_service.get_document(
            document_id=feature_job_setting_analysis.event_data_id
        )

        # get workspace info
        workspace = await self.workspace_service.get_document(
            feature_job_setting_analysis.workspace_id
        )

        return FeatureJobSettingAnalysisInfo(
            created_at=feature_job_setting_analysis.created_at,
            event_data_name=event_data.name,
            analysis_options=feature_job_setting_analysis.analysis_options,
            analysis_parameters=feature_job_setting_analysis.analysis_parameters,
            recommendation=FeatureJobSetting(
                blind_spot=f"{recommended_setting.blind_spot}s",
                time_modulo_frequency=f"{recommended_setting.job_time_modulo_frequency}s",
                frequency=f"{recommended_setting.frequency}s",
            ),
            workspace_name=workspace.name,
        )

    async def get_workspace_info(self, document_id: ObjectId, verbose: bool) -> WorkspaceInfo:
        """
        Get workspace info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        WorkspaceInfo
        """
        _ = verbose
        workspace = await self.workspace_service.get_document(document_id=document_id)
        return WorkspaceInfo(
            name=workspace.name,
            created_at=workspace.created_at,
            updated_at=workspace.updated_at,
        )
