"""
InfoService class
"""
from __future__ import annotations

from typing import Any, Optional, Type, TypeVar

from bson.objectid import ObjectId

from featurebyte import TableCleaningOperation
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSetting,
    TableFeatureJobSetting,
)
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure
from featurebyte.schema.feature import FeatureBriefInfoList
from featurebyte.schema.info import (
    EntityBriefInfoList,
    FeatureInfo,
    FeatureNamespaceInfo,
    TableBriefInfoList,
)
from featurebyte.schema.semantic import SemanticList
from featurebyte.schema.table import TableList
from featurebyte.service.base_document import BaseDocumentService, DocumentUpdateSchema
from featurebyte.service.base_service import BaseService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.entity import EntityService, get_primary_entity_from_entities
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.mixin import Document, DocumentCreateSchema
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table import TableService

ObjectT = TypeVar("ObjectT")


class InfoService(BaseService):
    """
    InfoService class is responsible for rendering the info of a specific api object.
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        table_service: TableService,
        semantic_service: SemanticService,
        catalog_service: CatalogService,
        entity_service: EntityService,
        feature_service: FeatureService,
        feature_namespace_service: FeatureNamespaceService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.table_service = table_service
        self.semantic_service = semantic_service
        self.catalog_service = catalog_service
        self.entity_service = entity_service
        self.feature_service = feature_service
        self.feature_namespace_service = feature_namespace_service

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
        primary_entity = get_primary_entity_from_entities(entities=entities)

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
