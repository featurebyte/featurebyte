"""
InfoService class
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.schema.common.operation import DictProject
from featurebyte.schema.data import DataBriefInfoList
from featurebyte.schema.dimension_data import DimensionDataColumnInfo, DimensionDataInfo
from featurebyte.schema.entity import EntityBriefInfoList, EntityInfo
from featurebyte.schema.event_data import EventDataColumnInfo, EventDataInfo
from featurebyte.schema.feature import FeatureBriefInfoList, FeatureInfo
from featurebyte.schema.feature_list import FeatureListBriefInfoList, FeatureListInfo
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceInfo
from featurebyte.schema.feature_namespace import FeatureNamespaceInfo
from featurebyte.schema.feature_store import FeatureStoreInfo
from featurebyte.service.base_service import BaseService, DocServiceName


class InfoService(BaseService):
    """
    InfoService class is responsible for rendering the info of a specific api object.
    """

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
        entity_ids = DictProject(rule=("columns_info", "entity_id")).project(event_data.dict())
        entities = await self.entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": entity_ids}}
        )
        columns_info = None
        if verbose:
            columns_info = []
            entity_map = {entity["_id"]: entity["name"] for entity in entities["data"]}
            for column_info in event_data.columns_info:
                columns_info.append(
                    EventDataColumnInfo(
                        **column_info.dict(), entity=entity_map.get(column_info.entity_id)
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
        dimension_data = await self.get_document(DocServiceName.DIMENSION_DATA, document_id)
        entity_ids = DictProject(rule=("columns_info", "entity_id")).project(dimension_data.dict())
        entities = await self.entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": entity_ids}}
        )
        columns_info = None
        if verbose:
            columns_info = []
            entity_map = {entity["_id"]: entity["name"] for entity in entities["data"]}
            for column_info in dimension_data.columns_info:
                columns_info.append(
                    DimensionDataColumnInfo(
                        **column_info.dict(), entity=entity_map.get(column_info.entity_id)
                    )
                )

        return DimensionDataInfo(
            dimension_data_primary_key_column=dimension_data.dimension_data_primary_key_column,
            columns_info=columns_info,
        )

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

        return FeatureInfo(
            **namespace_info.dict(),
            version={"this": feature.version.to_str(), "default": default_feature.version.to_str()},
            readiness={"this": feature.readiness, "default": default_feature.readiness},
            versions_info=versions_info,
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
