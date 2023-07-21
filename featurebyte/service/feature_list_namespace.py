"""
FeatureListNamespaceService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId

from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.persistent import Persistent
from featurebyte.routes.catalog.catalog_name_injector import CatalogNameInjector
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceServiceUpdate
from featurebyte.schema.info import (
    EntityBriefInfoList,
    FeatureListNamespaceInfo,
    TableBriefInfoList,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService, get_primary_entity_from_entities
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.table import TableService


class FeatureListNamespaceService(
    BaseDocumentService[
        FeatureListNamespaceModel, FeatureListNamespaceModel, FeatureListNamespaceServiceUpdate
    ],
):
    """
    FeatureListNamespaceService class
    """

    document_class = FeatureListNamespaceModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        entity_service: EntityService,
        table_service: TableService,
        feature_namespace_service: FeatureNamespaceService,
        catalog_name_injector: CatalogNameInjector,
    ):
        super().__init__(user, persistent, catalog_id)
        self.entity_service = entity_service
        self.table_service = table_service
        self.feature_namespace_service = feature_namespace_service
        self.catalog_name_injector = catalog_name_injector

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
        namespace = await self.get_document(document_id=document_id)
        entities = await self.entity_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.entity_ids}}
        )
        primary_entity = get_primary_entity_from_entities(entities)

        tables = await self.table_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.table_ids}}
        )

        # get catalog info
        catalog_name, updated_docs = await self.catalog_name_injector.add_name(
            namespace.catalog_id, [entities, tables]
        )
        entities, tables = updated_docs

        # get default feature ids
        feat_namespace_to_default_id = {}
        async for feat_namespace in self.feature_namespace_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": namespace.feature_namespace_ids}}
        ):
            feat_namespace_to_default_id[feat_namespace["_id"]] = feat_namespace[
                "default_feature_id"
            ]

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
            catalog_name=catalog_name,
            feature_namespace_ids=namespace.feature_namespace_ids,
            default_feature_ids=[
                feat_namespace_to_default_id[feat_namespace_id]
                for feat_namespace_id in namespace.feature_namespace_ids
            ],
            description=namespace.description,
        )
