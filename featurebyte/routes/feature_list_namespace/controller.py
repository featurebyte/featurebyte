"""
FeatureListNamespace API route controller
"""

from __future__ import annotations

import copy
from typing import Any, cast

from bson import ObjectId

from featurebyte.models.feature_list_namespace import FeatureListNamespaceModel
from featurebyte.persistent.base import SortDir
from featurebyte.routes.catalog.catalog_name_injector import CatalogNameInjector
from featurebyte.routes.common.base import BaseDocumentController, PaginatedDocument
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceList,
    FeatureListNamespaceModelResponse,
    FeatureListNamespaceUpdate,
)
from featurebyte.schema.info import (
    EntityBriefInfoList,
    FeatureListNamespaceInfo,
    TableBriefInfoList,
)
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_facade import FeatureListFacadeService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE, Document
from featurebyte.service.table import TableService


class FeatureListNamespaceController(
    BaseDocumentController[
        FeatureListNamespaceModelResponse, FeatureListNamespaceService, FeatureListNamespaceList
    ]
):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListNamespaceList

    def __init__(
        self,
        feature_list_namespace_service: FeatureListNamespaceService,
        feature_list_service: FeatureListService,
        feature_list_facade_service: FeatureListFacadeService,
        feature_namespace_service: FeatureNamespaceService,
        entity_service: EntityService,
        table_service: TableService,
        catalog_name_injector: CatalogNameInjector,
    ):
        super().__init__(feature_list_namespace_service)
        self.feature_list_service = feature_list_service
        self.feature_list_facade_service = feature_list_facade_service
        self.feature_namespace_service = feature_namespace_service
        self.entity_service = entity_service
        self.table_service = table_service
        self.catalog_name_injector = catalog_name_injector

    async def get(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
    ) -> Document:
        document = await self.service.get_document(
            document_id=document_id, exception_detail=exception_detail
        )
        default_feature_list_doc = await self.feature_list_service.get_document_as_dict(
            document_id=document.default_feature_list_id,
            projection={
                "_id": 1,
                "primary_entity_ids": 1,
                "entity_ids": 1,
                "table_ids": 1,
                "readiness_distribution": 1,
                "dtype_distribution": 1,
            },
        )
        output = FeatureListNamespaceModelResponse(**{
            **document.model_dump(by_alias=True),
            "primary_entity_ids": default_feature_list_doc["primary_entity_ids"],
            "entity_ids": default_feature_list_doc["entity_ids"],
            "table_ids": default_feature_list_doc["table_ids"],
            "readiness_distribution": default_feature_list_doc["readiness_distribution"],
            "dtype_distribution": default_feature_list_doc["dtype_distribution"],
        })
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

        # compute primary entity ids of each feature list namespace
        default_feature_list_ids = [
            document["default_feature_list_id"] for document in document_data["data"]
        ]
        feature_list_id_to_doc = {
            doc["_id"]: doc
            async for doc in self.feature_list_service.list_documents_as_dict_iterator(
                query_filter={"_id": {"$in": default_feature_list_ids}},
                projection={
                    "_id": 1,
                    "primary_entity_ids": 1,
                    "entity_ids": 1,
                    "table_ids": 1,
                    "readiness_distribution": 1,
                    "dtype_distribution": 1,
                },
            )
        }
        output = []
        for feature_list_namespace in document_data["data"]:
            feature_list_doc = feature_list_id_to_doc[
                feature_list_namespace["default_feature_list_id"]
            ]
            output.append(
                FeatureListNamespaceModelResponse(**{
                    **feature_list_namespace,
                    "primary_entity_ids": feature_list_doc["primary_entity_ids"],
                    "entity_ids": feature_list_doc["entity_ids"],
                    "table_ids": feature_list_doc["table_ids"],
                    "readiness_distribution": feature_list_doc["readiness_distribution"],
                    "dtype_distribution": feature_list_doc["dtype_distribution"],
                })
            )

        document_data["data"] = output
        return cast(PaginatedDocument, self.paginated_document_class(**document_data))

    async def update_feature_list_namespace(
        self,
        feature_list_namespace_id: ObjectId,
        data: FeatureListNamespaceUpdate,
    ) -> FeatureListNamespaceModel:
        """
        Update FeatureListNamespace stored at persistent (GitDB or MongoDB)

        Parameters
        ----------
        feature_list_namespace_id: ObjectId
            FeatureListNamespace ID
        data: FeatureListNamespaceUpdate
            FeatureListNamespace update payload

        Returns
        -------
        FeatureListNamespaceModel
            FeatureListNamespace object with updated attribute(s)
        """
        if data.status:
            await self.feature_list_facade_service.update_status(
                feature_list_namespace_id=feature_list_namespace_id,
                status=data.status,
            )

        return await self.get(document_id=feature_list_namespace_id)

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> FeatureListNamespaceInfo:
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
        feature_list = await self.feature_list_service.get_document_as_dict(
            document_id=namespace.default_feature_list_id
        )
        entities = await self.entity_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": feature_list["entity_ids"]}}
        )
        tables = await self.table_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": feature_list["table_ids"]}}
        )
        # get catalog info
        catalog_name, updated_docs = await self.catalog_name_injector.add_name(
            namespace.catalog_id, [entities, tables]
        )
        entities, tables = updated_docs
        primary_entity_data = copy.deepcopy(entities)
        primary_entity_data["data"] = sorted(
            [
                entity
                for entity in entities["data"]
                if entity["_id"] in feature_list["primary_entity_ids"]
            ],
            key=lambda doc: doc["_id"],  # type: ignore
        )

        # get default feature ids
        feat_namespace_to_default_id = {}
        async for feat_namespace in self.feature_namespace_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": namespace.feature_namespace_ids}},
            projection={"_id": 1, "default_feature_id": 1},
        ):
            feat_namespace_to_default_id[feat_namespace["_id"]] = feat_namespace[
                "default_feature_id"
            ]

        return FeatureListNamespaceInfo(
            name=namespace.name,
            readiness_distribution=feature_list["readiness_distribution"],
            dtype_distribution=feature_list["dtype_distribution"],
            created_at=namespace.created_at,
            updated_at=namespace.updated_at,
            entities=EntityBriefInfoList.from_paginated_data(entities),
            primary_entity=EntityBriefInfoList.from_paginated_data(
                paginated_data=primary_entity_data
            ),
            tables=TableBriefInfoList.from_paginated_data(tables),
            default_feature_list_id=namespace.default_feature_list_id,
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
