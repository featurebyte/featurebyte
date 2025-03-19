"""
FeatureNamespace API route controller
"""

from __future__ import annotations

import copy
from typing import Any, cast

from bson import ObjectId

from featurebyte.persistent.base import SortDir
from featurebyte.routes.catalog.catalog_name_injector import CatalogNameInjector
from featurebyte.routes.common.base import BaseDocumentController, PaginatedDocument
from featurebyte.routes.common.feature_or_target_helper import FeatureOrTargetHelper
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceList,
    FeatureNamespaceModelResponse,
    FeatureNamespaceServiceUpdate,
    FeatureNamespaceUpdate,
)
from featurebyte.schema.info import EntityBriefInfoList, FeatureNamespaceInfo, TableBriefInfoList
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_facade import FeatureFacadeService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_type import FeatureTypeService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE, Document
from featurebyte.service.table import TableService


class FeatureNamespaceController(
    BaseDocumentController[
        FeatureNamespaceModelResponse, FeatureNamespaceService, FeatureNamespaceList
    ]
):
    """
    FeatureName controller
    """

    paginated_document_class = FeatureNamespaceList

    def __init__(
        self,
        feature_namespace_service: FeatureNamespaceService,
        feature_facade_service: FeatureFacadeService,
        entity_service: EntityService,
        feature_service: FeatureService,
        table_service: TableService,
        catalog_name_injector: CatalogNameInjector,
        feature_or_target_helper: FeatureOrTargetHelper,
        feature_type_service: FeatureTypeService,
    ):
        super().__init__(feature_namespace_service)
        self.feature_facade_service = feature_facade_service
        self.entity_service = entity_service
        self.feature_service = feature_service
        self.table_service = table_service
        self.catalog_name_injector = catalog_name_injector
        self.feature_or_target_helper = feature_or_target_helper
        self.feature_type_service = feature_type_service

    async def get(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
    ) -> Document:
        document = await self.service.get_document(
            document_id=document_id,
            exception_detail=exception_detail,
        )
        default_feature = await self.feature_service.get_document(
            document_id=document.default_feature_id
        )
        output = FeatureNamespaceModelResponse(
            **document.model_dump(by_alias=True),
            primary_table_ids=default_feature.primary_table_ids,
            primary_entity_ids=default_feature.primary_entity_ids,
        )
        return cast(Document, output)

    async def list(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: list[tuple[str, SortDir]] | None = None,
        **kwargs: Any,
    ) -> PaginatedDocument:
        sort_by = sort_by or [("created_at", "desc")]
        document_data = await self.service.list_documents_as_dict(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            **kwargs,
        )

        # get all the default features & entities
        default_feature_ids = set(
            document["default_feature_id"] for document in document_data["data"]
        )

        feature_id_to_primary_table_ids = {}
        feature_id_to_primary_entity_ids = {}
        async for feature in self.feature_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": list(default_feature_ids)}},
            projection={"primary_table_ids": 1, "primary_entity_ids": 1},
        ):
            feature_id_to_primary_table_ids[feature["_id"]] = feature["primary_table_ids"]
            feature_id_to_primary_entity_ids[feature["_id"]] = feature["primary_entity_ids"]

        # construct primary entity IDs and primary table IDs & add these attributes to feature namespace docs
        output = []
        for feature_namespace in document_data["data"]:
            default_feature_id = feature_namespace["default_feature_id"]
            primary_entity_ids = feature_id_to_primary_entity_ids.get(default_feature_id, [])
            primary_table_ids = feature_id_to_primary_table_ids.get(default_feature_id, [])
            output.append(
                FeatureNamespaceModelResponse(
                    **feature_namespace,
                    primary_entity_ids=primary_entity_ids,
                    primary_table_ids=primary_table_ids,
                )
            )

        document_data["data"] = output
        return cast(PaginatedDocument, self.paginated_document_class(**document_data))

    async def update_feature_namespace(
        self,
        feature_namespace_id: ObjectId,
        data: FeatureNamespaceUpdate,
    ) -> FeatureNamespaceModelResponse:
        """
        Update FeatureNamespace stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        feature_namespace_id: ObjectId
            FeatureNamespace ID
        data: FeatureNamespaceUpdate
            FeatureNamespace update payload

        Returns
        -------
        FeatureNamespaceModelResponse
            FeatureNamespace object with updated attribute(s)
        """
        if data.default_version_mode:
            await self.feature_facade_service.update_default_version_mode(
                feature_namespace_id=feature_namespace_id,
                default_version_mode=data.default_version_mode,
            )

        if data.default_feature_id:
            await self.feature_facade_service.update_default_feature(
                feature_id=data.default_feature_id
            )

        if data.feature_type:
            namespace = await self.service.get_document(document_id=feature_namespace_id)
            default_feature = await self.feature_service.get_document(
                document_id=namespace.default_feature_id
            )
            self.feature_type_service.validate_feature_type(
                feature=default_feature, feature_type=data.feature_type
            )
            await self.service.update_document(
                document_id=feature_namespace_id,
                data=FeatureNamespaceServiceUpdate(feature_type=data.feature_type),
            )

        return await self.get(document_id=feature_namespace_id)

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> FeatureNamespaceInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        InfoDocument
        """
        _ = verbose
        namespace = await self.service.get_document(document_id=document_id)

        tables = await self.table_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.table_ids}}
        )
        feature = await self.feature_service.get_document(document_id=namespace.default_feature_id)
        entities = await self.entity_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": feature.entity_ids}}
        )

        # Add catalog name to entities and tables
        catalog_name, updated_docs = await self.catalog_name_injector.add_name(
            namespace.catalog_id, [entities, tables]
        )
        entities, tables = updated_docs

        # prepare primary entity & primary table
        primary_entity = copy.deepcopy(entities)
        primary_entity["data"] = [
            entity for entity in entities["data"] if entity["_id"] in feature.primary_entity_ids
        ]
        primary_tables = await self.feature_or_target_helper.get_primary_tables(
            namespace.table_ids,
            namespace.catalog_id,
            feature.graph,
            feature.node_name,
        )

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
            catalog_name=catalog_name,
            description=namespace.description,
            feature_type=namespace.feature_type,
        )
