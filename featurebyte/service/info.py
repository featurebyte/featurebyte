"""
InfoService class
"""
from __future__ import annotations

from typing import Any, Optional, Type, TypeVar

from bson.objectid import ObjectId

from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.tabular_data import TabularDataModel
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure
from featurebyte.schema.feature import FeatureBriefInfoList
from featurebyte.schema.info import (
    DataBriefInfoList,
    EntityBriefInfoList,
    EntityInfo,
    EventDataColumnInfo,
    EventDataInfo,
    FeatureInfo,
    FeatureListBriefInfoList,
    FeatureListInfo,
    FeatureListNamespaceInfo,
    FeatureNamespaceInfo,
    FeatureStoreInfo,
)
from featurebyte.schema.semantic import SemanticList
from featurebyte.schema.tabular_data import TabularDataList
from featurebyte.service.base_document import BaseDocumentService, DocumentUpdateSchema
from featurebyte.service.base_service import BaseService, DocServiceName
from featurebyte.service.mixin import Document, DocumentCreateSchema

ObjectT = TypeVar("ObjectT")


class InfoService(BaseService):
    """
    InfoService class is responsible for rendering the info of a specific api object.
    """

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
        feature_store = await self.get_document(
            DocServiceName.FEATURE_STORE, document_id=document_id
        )
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
        entity = await self.get_document(DocServiceName.ENTITY, document_id)
        return EntityInfo(
            name=entity.name,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
            serving_names=entity.serving_names,
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
        event_data = await self.get_document(DocServiceName.EVENT_DATA, document_id)
        entities = await self.entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": event_data.entity_ids}}
        )
        columns_info = None
        if verbose:
            columns_info = []
            entity_map = {ObjectId(entity["_id"]): entity["name"] for entity in entities["data"]}
            for column_info in event_data.columns_info:
                columns_info.append(
                    EventDataColumnInfo(
                        **column_info.dict(), entity=entity_map.get(column_info.entity_id)  # type: ignore
                    )
                )

        return EventDataInfo(
            name=event_data.name,
            created_at=event_data.created_at,
            updated_at=event_data.updated_at,
            event_timestamp_column=event_data.event_timestamp_column,
            record_creation_date_column=event_data.record_creation_date_column,
            table_details=event_data.tabular_source.table_details,
            default_feature_job_setting=event_data.default_feature_job_setting,
            status=event_data.status,
            entities=EntityBriefInfoList.from_paginated_data(entities),
            column_count=len(event_data.columns_info),
            columns_info=columns_info,
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
                "groupby": agg_col.groupby,
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
        feature = await self.get_document(DocServiceName.FEATURE, document_id)
        namespace_info = await self.get_feature_namespace_info(
            document_id=feature.feature_namespace_id,
            verbose=verbose,
        )
        default_feature = await self.get_document(
            DocServiceName.FEATURE, namespace_info.default_feature_id
        )
        versions_info = None
        if verbose:
            namespace = await self.get_document(
                DocServiceName.FEATURE_NAMESPACE, feature.feature_namespace_id
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
        namespace = await self.get_document(DocServiceName.FEATURE_NAMESPACE, document_id)
        entities = await self.entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.entity_ids}}
        )

        tabular_data = await self.data_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.tabular_data_ids}}
        )
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
        feature_list = await self.get_document(DocServiceName.FEATURE_LIST, document_id)
        namespace_info = await self.get_feature_list_namespace_info(
            document_id=feature_list.feature_list_namespace_id,
            verbose=verbose,
        )
        default_feature_list = await self.get_document(
            DocServiceName.FEATURE_LIST, namespace_info.default_feature_list_id
        )
        versions_info = None
        if verbose:
            namespace = await self.get_document(
                DocServiceName.FEATURE_LIST_NAMESPACE, feature_list.feature_list_namespace_id
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
        namespace = await self.get_document(DocServiceName.FEATURE_LIST_NAMESPACE, document_id)
        entities = await self.entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.entity_ids}}
        )

        tabular_data = await self.data_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.tabular_data_ids}}
        )
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
        )
