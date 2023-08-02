"""
FeatureNamespace API route controller
"""
from __future__ import annotations

from typing import Any, Literal, Optional, cast

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature_namespace import DefaultVersionMode, FeatureReadiness
from featurebyte.routes.catalog.catalog_name_injector import CatalogNameInjector
from featurebyte.routes.common.base import (
    BaseDocumentController,
    DerivePrimaryEntityHelper,
    PaginatedDocument,
)
from featurebyte.routes.common.feature_or_target_helper import FeatureOrTargetHelper
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceList,
    FeatureNamespaceModelResponse,
    FeatureNamespaceServiceUpdate,
    FeatureNamespaceUpdate,
)
from featurebyte.schema.info import EntityBriefInfoList, FeatureNamespaceInfo, TableBriefInfoList
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.entity import EntityService, get_primary_entity_from_entities
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
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
        entity_service: EntityService,
        feature_service: FeatureService,
        default_version_mode_service: DefaultVersionModeService,
        feature_readiness_service: FeatureReadinessService,
        table_service: TableService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
        catalog_name_injector: CatalogNameInjector,
        feature_or_target_helper: FeatureOrTargetHelper,
    ):
        super().__init__(feature_namespace_service)
        self.entity_service = entity_service
        self.feature_service = feature_service
        self.default_version_mode_service = default_version_mode_service
        self.feature_readiness_service = feature_readiness_service
        self.table_service = table_service
        self.derive_primary_entity_helper = derive_primary_entity_helper
        self.catalog_name_injector = catalog_name_injector
        self.feature_or_target_helper = feature_or_target_helper

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
            **document.dict(by_alias=True),
            primary_table_ids=default_feature.primary_table_ids,
            primary_entity_ids=await self.derive_primary_entity_helper.derive_primary_entity_ids(
                entity_ids=document.entity_ids
            ),
        )
        return cast(Document, output)

    async def list(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> PaginatedDocument:
        document_data = await self.service.list_documents_as_dict(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            **kwargs,
        )

        # get all the default features & entities
        default_feature_ids = set(
            document["default_feature_id"] for document in document_data["data"]
        )
        entity_id_to_entity = await self.derive_primary_entity_helper.get_entity_id_to_entity(
            doc_list=document_data["data"]
        )

        feature_id_to_primary_table_ids = {}
        async for feature in self.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(default_feature_ids)}}
        ):
            feature_id_to_primary_table_ids[feature.id] = feature.primary_table_ids

        # construct primary entity IDs and primary table IDs & add these attributes to feature namespace docs
        output = []
        for feature_namespace in document_data["data"]:
            primary_entity_ids = await self.derive_primary_entity_helper.derive_primary_entity_ids(
                entity_ids=feature_namespace["entity_ids"], entity_id_to_entity=entity_id_to_entity
            )
            default_feature_id = feature_namespace["default_feature_id"]
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

        Raises
        ------
        DocumentUpdateError
            When the new feature version creation fails
        """
        if data.default_version_mode:
            await self.default_version_mode_service.update_feature_namespace(
                feature_namespace_id=feature_namespace_id,
                default_version_mode=data.default_version_mode,
                return_document=False,
            )

        if data.default_feature_id:
            feature_namespace = await self.service.get_document(document_id=feature_namespace_id)
            if feature_namespace.default_version_mode != DefaultVersionMode.MANUAL:
                raise DocumentUpdateError(
                    "Cannot set default feature ID when default version mode is not MANUAL."
                )

            # check new default feature ID exists & make sure it is the highest readiness level among all versions
            new_default_feature = await self.feature_service.get_document(
                document_id=data.default_feature_id
            )
            max_readiness = FeatureReadiness(new_default_feature.readiness)
            version: Optional[str] = None
            async for feature_dict in self.feature_service.list_documents_as_dict_iterator(
                query_filter={"_id": {"$in": feature_namespace.feature_ids}}
            ):
                max_readiness = max(max_readiness, FeatureReadiness(feature_dict["readiness"]))
                if feature_dict["readiness"] == max_readiness:
                    version = VersionIdentifier(**feature_dict["version"]).to_str()

            if new_default_feature.readiness != max_readiness:
                raise DocumentUpdateError(
                    f"Cannot set default feature ID to {new_default_feature.id} "
                    f"because its readiness level ({new_default_feature.readiness}) "
                    f"is lower than the readiness level of version {version} ({max_readiness.value})."
                )

            # update feature namespace default feature ID and update feature readiness
            await self.service.update_document(
                document_id=feature_namespace_id,
                data=FeatureNamespaceServiceUpdate(default_feature_id=data.default_feature_id),
            )
            await self.feature_readiness_service.update_feature_namespace(
                feature_namespace_id=feature_namespace_id
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
        entities = await self.entity_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.entity_ids}}
        )
        primary_entity = get_primary_entity_from_entities(entities=entities)

        tables = await self.table_service.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": namespace.table_ids}}
        )

        # Add catalog name to entities and tables
        catalog_name, updated_docs = await self.catalog_name_injector.add_name(
            namespace.catalog_id, [entities, tables]
        )
        entities, tables = updated_docs

        # derive primary tables
        feature = await self.feature_service.get_document(document_id=namespace.default_feature_id)
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
        )
