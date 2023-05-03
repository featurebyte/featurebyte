"""
InfoService class
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Type, TypeVar

import copy

from bson.objectid import ObjectId

from featurebyte import TableCleaningOperation
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.entity import EntityModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_store import TableModel
from featurebyte.models.relationship_analysis import derive_primary_entity
from featurebyte.persistent import Persistent
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSetting,
    TableFeatureJobSetting,
)
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure
from featurebyte.schema.feature import FeatureBriefInfoList
from featurebyte.schema.info import (
    BatchFeatureTableInfo,
    BatchRequestTableInfo,
    CatalogInfo,
    CredentialInfo,
    DeploymentInfo,
    DimensionTableInfo,
    EntityBriefInfoList,
    EntityInfo,
    EventTableInfo,
    FeatureInfo,
    FeatureJobSettingAnalysisInfo,
    FeatureListBriefInfoList,
    FeatureListInfo,
    FeatureListNamespaceInfo,
    FeatureNamespaceInfo,
    FeatureStoreInfo,
    HistoricalFeatureTableInfo,
    ItemTableInfo,
    ObservationTableInfo,
    SCDTableInfo,
    TableBriefInfoList,
)
from featurebyte.schema.relationship_info import RelationshipInfoInfo
from featurebyte.schema.semantic import SemanticList
from featurebyte.schema.table import TableList
from featurebyte.service.base_document import BaseDocumentService, DocumentUpdateSchema
from featurebyte.service.base_service import BaseService
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.context import ContextService
from featurebyte.service.credential import CredentialService
from featurebyte.service.deployment import DeploymentService
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.mixin import Document, DocumentCreateSchema
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.relationship_info import RelationshipInfoService
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table import TableService
from featurebyte.service.user_service import UserService

ObjectT = TypeVar("ObjectT")


class InfoService(BaseService):
    """
    InfoService class is responsible for rendering the info of a specific api object.
    """

    # pylint: disable=too-many-instance-attributes,too-many-lines

    def __init__(self, user: Any, persistent: Persistent, catalog_id: ObjectId):
        super().__init__(user, persistent, catalog_id)
        self.table_service = TableService(user=user, persistent=persistent, catalog_id=catalog_id)
        self.event_table_service = EventTableService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.item_table_service = ItemTableService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.dimension_table_service = DimensionTableService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.scd_table_service = SCDTableService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.semantic_service = SemanticService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.feature_store_service = FeatureStoreService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.entity_service = EntityService(user=user, persistent=persistent, catalog_id=catalog_id)
        self.feature_service = FeatureService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.feature_namespace_service = FeatureNamespaceService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.feature_list_service = FeatureListService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.feature_list_namespace_service = FeatureListNamespaceService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.feature_job_setting_analysis_service = FeatureJobSettingAnalysisService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.catalog_service = CatalogService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.credential_service = CredentialService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.relationship_info_service = RelationshipInfoService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.context_service = ContextService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.observation_table_service = ObservationTableService(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            feature_store_service=self.feature_store_service,
            context_service=self.context_service,
        )
        self.historical_feature_table_service = HistoricalFeatureTableService(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            feature_store_service=self.feature_store_service,
        )
        self.deployment_service = DeploymentService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        self.batch_request_table_service = BatchRequestTableService(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            feature_store_service=self.feature_store_service,
            context_service=self.context_service,
        )
        self.batch_feature_table_service = BatchFeatureTableService(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            feature_store_service=self.feature_store_service,
        )
        self.user_service = UserService(user=user, persistent=persistent, catalog_id=catalog_id)

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

        # get catalog info
        catalog = await self.catalog_service.get_document(entity.catalog_id)

        return EntityInfo(
            name=entity.name,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
            serving_names=entity.serving_names,
            catalog_name=catalog.name,
        )

    async def _get_table_info(self, data_document: TableModel, verbose: bool) -> Dict[str, Any]:
        """
        Get table info

        Parameters
        ----------
        data_document: TableModel
            Data document (could be event table, SCD table, item table, dimension table, etc)
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

        # get catalog info
        catalog = await self.catalog_service.get_document(data_document.catalog_id)
        for entity in entities["data"]:
            assert entity["catalog_id"] == catalog.id
            entity["catalog_name"] = catalog.name

        return {
            "name": data_document.name,
            "created_at": data_document.created_at,
            "updated_at": data_document.updated_at,
            "record_creation_timestamp_column": data_document.record_creation_timestamp_column,
            "table_details": data_document.tabular_source.table_details,
            "status": data_document.status,
            "entities": EntityBriefInfoList.from_paginated_data(entities),
            "semantics": [semantic["name"] for semantic in semantics["data"]],
            "column_count": len(data_document.columns_info),
            "columns_info": columns_info,
            "catalog_name": catalog.name,
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
        table_info = await self.table_service.get_document(
            document_id=relationship_info.relation_table_id
        )
        updated_user_name = self.user_service.get_user_name_for_id(relationship_info.updated_by)
        entity = await self.entity_service.get_document(document_id=relationship_info.entity_id)
        related_entity = await self.entity_service.get_document(
            document_id=relationship_info.related_entity_id
        )
        return RelationshipInfoInfo(
            id=relationship_info.id,
            name=relationship_info.name,
            created_at=relationship_info.created_at,
            updated_at=relationship_info.updated_at,
            relationship_type=relationship_info.relationship_type,
            table_name=table_info.name,
            data_type=table_info.type,
            entity_name=entity.name,
            related_entity_name=related_entity.name,
            updated_by=updated_user_name,
        )

    async def get_event_table_info(self, document_id: ObjectId, verbose: bool) -> EventTableInfo:
        """
        Get event table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        EventTableInfo
        """
        event_table = await self.event_table_service.get_document(document_id=document_id)
        table_dict = await self._get_table_info(data_document=event_table, verbose=verbose)
        return EventTableInfo(
            **table_dict,
            event_id_column=event_table.event_id_column,
            event_timestamp_column=event_table.event_timestamp_column,
            default_feature_job_setting=event_table.default_feature_job_setting,
        )

    async def get_item_table_info(self, document_id: ObjectId, verbose: bool) -> ItemTableInfo:
        """
        Get item table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        ItemTableInfo
        """
        item_table = await self.item_table_service.get_document(document_id=document_id)
        table_dict = await self._get_table_info(data_document=item_table, verbose=verbose)
        event_table = await self.event_table_service.get_document(
            document_id=item_table.event_table_id
        )
        return ItemTableInfo(
            **table_dict,
            event_id_column=item_table.event_id_column,
            item_id_column=item_table.item_id_column,
            event_table_name=event_table.name,
        )

    async def get_dimension_table_info(
        self, document_id: ObjectId, verbose: bool
    ) -> DimensionTableInfo:
        """
        Get dimension table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        DimensionTableInfo
        """
        dimension_table = await self.dimension_table_service.get_document(document_id=document_id)
        table_dict = await self._get_table_info(data_document=dimension_table, verbose=verbose)
        return DimensionTableInfo(
            **table_dict,
            dimension_id_column=dimension_table.dimension_id_column,
        )

    async def get_scd_table_info(self, document_id: ObjectId, verbose: bool) -> SCDTableInfo:
        """
        Get Slow Changing Dimension table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        SCDTableInfo
        """
        scd_table = await self.scd_table_service.get_document(document_id=document_id)
        table_dict = await self._get_table_info(data_document=scd_table, verbose=verbose)
        return SCDTableInfo(
            **table_dict,
            natural_key_column=scd_table.natural_key_column,
            effective_timestamp_column=scd_table.effective_timestamp_column,
            surrogate_key_column=scd_table.surrogate_key_column,
            end_timestamp_column=scd_table.end_timestamp_column,
            current_flag_column=scd_table.current_flag_column,
        )

    async def _extract_feature_metadata(self, op_struct: GroupOperationStructure) -> dict[str, Any]:
        # retrieve related tables & semantics
        table_list = await self._get_list_object(self.table_service, op_struct.table_ids, TableList)
        semantic_list = await self._get_list_object(
            self.semantic_service, table_list.semantic_ids, SemanticList
        )

        # prepare column mapping
        column_map: dict[tuple[Optional[ObjectId], str], Any] = {}
        semantic_map = {semantic.id: semantic.name for semantic in semantic_list.data}
        for table in table_list.data:
            for column in table.columns_info:
                column_map[(table.id, column.name)] = {
                    "table_name": table.name,
                    "semantic": semantic_map.get(column.semantic_id),  # type: ignore
                }

        # construct feature metadata
        source_columns = {}
        reference_map: dict[Any, str] = {}
        for idx, src_col in enumerate(op_struct.source_columns):
            column_metadata = column_map[(src_col.table_id, src_col.name)]
            reference_map[src_col] = f"Input{idx}"
            source_columns[reference_map[src_col]] = {
                "data": column_metadata["table_name"],
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
        return {
            "input_columns": source_columns,
            "derived_columns": derived_columns,
            "aggregations": aggregation_columns,
            "post_aggregation": post_aggregation,
        }

    @staticmethod
    def _extract_feature_table_cleaning_operations(
        feature: FeatureModel, table_id_to_name: dict[ObjectId, str]
    ) -> list[TableCleaningOperation]:
        table_cleaning_operations: list[TableCleaningOperation] = []
        for view_graph_node in feature.graph.iterate_sorted_graph_nodes(
            graph_node_types=GraphNodeType.view_graph_node_types()
        ):
            view_metadata = view_graph_node.parameters.metadata  # type: ignore
            if view_metadata.column_cleaning_operations:
                table_cleaning_operations.append(
                    TableCleaningOperation(
                        table_name=table_id_to_name[view_metadata.table_id],
                        column_cleaning_operations=view_metadata.column_cleaning_operations,
                    )
                )
        return table_cleaning_operations

    @staticmethod
    def _extract_table_feature_job_settings(
        feature: FeatureModel, table_id_to_name: dict[ObjectId, str]
    ) -> list[TableFeatureJobSetting]:
        table_feature_job_settings = []
        for group_by_node, data_id in feature.graph.iterate_group_by_node_and_table_id_pairs(
            target_node=feature.node
        ):
            assert data_id is not None, "Event table ID not found"
            table_name = table_id_to_name[data_id]
            group_by_node_params = group_by_node.parameters
            table_feature_job_settings.append(
                TableFeatureJobSetting(
                    table_name=table_name,
                    feature_job_setting=FeatureJobSetting(
                        blind_spot=f"{group_by_node_params.blind_spot}s",
                        frequency=f"{group_by_node_params.frequency}s",
                        time_modulo_frequency=f"{group_by_node_params.time_modulo_frequency}s",
                    ),
                )
            )
        return table_feature_job_settings

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
        catalog = await self.catalog_service.get_document(feature.catalog_id)
        data_id_to_doc = {}
        async for doc in self.table_service.list_documents_iterator(
            query_filter={"_id": {"$in": feature.table_ids}}
        ):
            doc["catalog_name"] = catalog.name
            data_id_to_doc[doc["_id"]] = doc

        data_id_to_name = {key: value["name"] for key, value in data_id_to_doc.items()}
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
        metadata = await self._extract_feature_metadata(op_struct=op_struct)
        return FeatureInfo(
            **namespace_info.dict(),
            version={"this": feature.version.to_str(), "default": default_feature.version.to_str()},
            readiness={"this": feature.readiness, "default": default_feature.readiness},
            table_feature_job_setting={
                "this": self._extract_table_feature_job_settings(
                    feature=feature, table_id_to_name=data_id_to_name
                ),
                "default": self._extract_table_feature_job_settings(
                    feature=default_feature, table_id_to_name=data_id_to_name
                ),
            },
            table_cleaning_operation={
                "this": self._extract_feature_table_cleaning_operations(
                    feature=feature, table_id_to_name=data_id_to_name
                ),
                "default": self._extract_feature_table_cleaning_operations(
                    feature=default_feature, table_id_to_name=data_id_to_name
                ),
            },
            versions_info=versions_info,
            metadata=metadata,
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
        primary_entity = self._get_primary_entity_from_entities(entities=entities)

        tables = await self.table_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.table_ids}}
        )

        # get catalog info
        catalog = await self.catalog_service.get_document(namespace.catalog_id)
        for entity in entities["data"]:
            assert entity["catalog_id"] == catalog.id
            entity["catalog_name"] = catalog.name
        for table in tables["data"]:
            assert table["catalog_id"] == catalog.id
            table["catalog_name"] = catalog.name

        # derive primary tables
        table_id_to_doc = {table["_id"]: table for table in tables["data"]}
        feature = await self.feature_service.get_document(document_id=namespace.default_feature_id)
        primary_input_nodes = feature.graph.get_primary_input_nodes(node_name=feature.node_name)
        primary_tables = [table_id_to_doc[node.parameters.id] for node in primary_input_nodes]

        return FeatureNamespaceInfo(
            name=namespace.name,
            created_at=namespace.created_at,
            updated_at=namespace.updated_at,
            entities=EntityBriefInfoList.from_paginated_data(entities),
            primary_entity=EntityBriefInfoList.from_paginated_data(primary_entity),
            tables=TableBriefInfoList.from_paginated_data(tables),
            primary_table=primary_tables,
            default_version_mode=namespace.default_version_mode,
            default_feature_id=namespace.default_feature_id,
            dtype=namespace.dtype,
            version_count=len(namespace.feature_ids),
            catalog_name=catalog.name,
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
        )

    def _get_primary_entity_from_entities(self, entities: dict[str, Any]) -> dict[str, Any]:
        """
        Get primary entity from entities data

        Parameters
        ----------
        entities: dict[str, Any]
            Entities listing result (with a "data" key and extras)

        Returns
        -------
        dict[str, Any]
            Filtered list of entities that are the main entities
        """
        main_entity_ids = {
            entity.id
            for entity in derive_primary_entity(
                [EntityModel(**entity_dict) for entity_dict in entities["data"]]
            )
        }
        primary_entity = copy.deepcopy(entities)
        primary_entity["data"] = [d for d in entities["data"] if d["_id"] in main_entity_ids]
        return primary_entity

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
        primary_entity = self._get_primary_entity_from_entities(entities)

        tables = await self.table_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.table_ids}}
        )

        # get catalog info
        catalog = await self.catalog_service.get_document(namespace.catalog_id)
        for entity in entities["data"]:
            assert entity["catalog_id"] == catalog.id
            entity["catalog_name"] = catalog.name
        for table in tables["data"]:
            assert table["catalog_id"] == catalog.id
            table["catalog_name"] = catalog.name

        return FeatureListNamespaceInfo(
            name=namespace.name,
            created_at=namespace.created_at,
            updated_at=namespace.updated_at,
            entities=EntityBriefInfoList.from_paginated_data(entities),
            primary_entity=EntityBriefInfoList.from_paginated_data(primary_entity),
            tables=TableBriefInfoList.from_paginated_data(tables),
            default_version_mode=namespace.default_version_mode,
            default_feature_list_id=namespace.default_feature_list_id,
            dtype_distribution=namespace.dtype_distribution,
            version_count=len(namespace.feature_list_ids),
            feature_count=len(namespace.feature_namespace_ids),
            status=namespace.status,
            catalog_name=catalog.name,
        )

    async def get_feature_job_setting_analysis_info(
        self, document_id: ObjectId, verbose: bool
    ) -> FeatureJobSettingAnalysisInfo:
        """
        Get item table info

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
        event_table = await self.event_table_service.get_document(
            document_id=feature_job_setting_analysis.event_table_id
        )

        # get catalog info
        catalog = await self.catalog_service.get_document(feature_job_setting_analysis.catalog_id)

        return FeatureJobSettingAnalysisInfo(
            created_at=feature_job_setting_analysis.created_at,
            event_table_name=event_table.name,
            analysis_options=feature_job_setting_analysis.analysis_options,
            analysis_parameters=feature_job_setting_analysis.analysis_parameters,
            recommendation=FeatureJobSetting(
                blind_spot=f"{recommended_setting.blind_spot}s",
                time_modulo_frequency=f"{recommended_setting.job_time_modulo_frequency}s",
                frequency=f"{recommended_setting.frequency}s",
            ),
            catalog_name=catalog.name,
        )

    async def get_catalog_info(self, document_id: ObjectId, verbose: bool) -> CatalogInfo:
        """
        Get catalog info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        CatalogInfo
        """
        _ = verbose
        catalog = await self.catalog_service.get_document(document_id=document_id)
        return CatalogInfo(
            name=catalog.name,
            created_at=catalog.created_at,
            updated_at=catalog.updated_at,
        )

    async def get_credential_info(self, document_id: ObjectId, verbose: bool) -> CredentialInfo:
        """
        Get credential info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        CredentialInfo
        """
        _ = verbose
        credential = await self.credential_service.get_document(document_id=document_id)
        return CredentialInfo(
            name=credential.name,
            feature_store_info=await self.get_feature_store_info(
                document_id=credential.feature_store_id, verbose=verbose
            ),
            database_credential_type=credential.database_credential.type
            if credential.database_credential
            else None,
            storage_credential_type=credential.storage_credential.type
            if credential.storage_credential
            else None,
            created_at=credential.created_at,
            updated_at=credential.updated_at,
        )

    async def get_observation_table_info(
        self, document_id: ObjectId, verbose: bool
    ) -> ObservationTableInfo:
        """
        Get observation table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        ObservationTableInfo
        """
        _ = verbose
        observation_table = await self.observation_table_service.get_document(
            document_id=document_id
        )
        feature_store = await self.feature_store_service.get_document(
            document_id=observation_table.location.feature_store_id
        )
        return ObservationTableInfo(
            name=observation_table.name,
            type=observation_table.request_input.type,
            feature_store_name=feature_store.name,
            table_details=observation_table.location.table_details,
            created_at=observation_table.created_at,
            updated_at=observation_table.updated_at,
        )

    async def get_historical_feature_table_info(
        self, document_id: ObjectId, verbose: bool
    ) -> HistoricalFeatureTableInfo:
        """
        Get historical feature table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        HistoricalFeatureTableInfo
        """
        _ = verbose
        historical_feature_table = await self.historical_feature_table_service.get_document(
            document_id=document_id
        )
        if historical_feature_table.observation_table_id is not None:
            observation_table = await self.observation_table_service.get_document(
                document_id=historical_feature_table.observation_table_id
            )
        else:
            observation_table = None
        feature_list = await self.feature_list_service.get_document(
            document_id=historical_feature_table.feature_list_id
        )
        return HistoricalFeatureTableInfo(
            name=historical_feature_table.name,
            feature_list_name=feature_list.name,
            feature_list_version=feature_list.version.to_str(),
            observation_table_name=observation_table.name if observation_table else None,
            table_details=historical_feature_table.location.table_details,
            created_at=historical_feature_table.created_at,
            updated_at=historical_feature_table.updated_at,
        )

    async def get_deployment_info(self, document_id: ObjectId, verbose: bool) -> DeploymentInfo:
        """
        Get deployment info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        DeploymentInfo
        """
        _ = verbose
        deployment = await self.deployment_service.get_document(document_id=document_id)
        feature_list = await self.feature_list_service.get_document(
            document_id=deployment.feature_list_id
        )
        return DeploymentInfo(
            name=deployment.name,
            feature_list_name=feature_list.name,
            feature_list_version=feature_list.version.to_str(),
            num_feature=len(feature_list.feature_ids),
            enabled=deployment.enabled,
            serving_endpoint=(
                f"/deployment/{deployment.id}/online_features" if deployment.enabled else None
            ),
            created_at=deployment.created_at,
            updated_at=deployment.updated_at,
        )

    async def get_batch_request_table_info(
        self, document_id: ObjectId, verbose: bool
    ) -> BatchRequestTableInfo:
        """
        Get batch request table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        BatchRequestTableInfo
        """
        _ = verbose
        batch_request_table = await self.batch_request_table_service.get_document(
            document_id=document_id
        )
        feature_store = await self.feature_store_service.get_document(
            document_id=batch_request_table.location.feature_store_id
        )
        return BatchRequestTableInfo(
            name=batch_request_table.name,
            type=batch_request_table.request_input.type,
            feature_store_name=feature_store.name,
            table_details=batch_request_table.location.table_details,
            created_at=batch_request_table.created_at,
            updated_at=batch_request_table.updated_at,
        )

    async def get_batch_feature_table_info(
        self, document_id: ObjectId, verbose: bool
    ) -> BatchFeatureTableInfo:
        """
        Get batch feature table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        BatchFeatureTableInfo
        """
        _ = verbose
        batch_feature_table = await self.batch_feature_table_service.get_document(
            document_id=document_id
        )
        batch_request_table = await self.batch_request_table_service.get_document(
            document_id=batch_feature_table.batch_request_table_id
        )
        deployment = await self.deployment_service.get_document(
            document_id=batch_feature_table.deployment_id
        )
        return BatchFeatureTableInfo(
            name=batch_feature_table.name,
            deployment_name=deployment.name,
            batch_request_table_name=batch_request_table.name,
            table_details=batch_feature_table.location.table_details,
            created_at=batch_feature_table.created_at,
            updated_at=batch_feature_table.updated_at,
        )
