"""
FeatureNamespace API route controller
"""
from __future__ import annotations

from typing import Any, Literal, cast

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature import DefaultVersionMode, FeatureModel
from featurebyte.routes.common.base import (
    BaseDocumentController,
    DerivePrimaryEntityMixin,
    PaginatedDocument,
)
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceList,
    FeatureNamespaceModelResponse,
    FeatureNamespaceServiceUpdate,
    FeatureNamespaceUpdate,
)
from featurebyte.schema.info import FeatureNamespaceInfo
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.info import InfoService
from featurebyte.service.mixin import Document


class FeatureNamespaceController(
    BaseDocumentController[
        FeatureNamespaceModelResponse, FeatureNamespaceService, FeatureNamespaceList
    ],
    DerivePrimaryEntityMixin,
):
    """
    FeatureName controller
    """

    paginated_document_class = FeatureNamespaceList

    def __init__(
        self,
        service: FeatureNamespaceService,
        entity_service: EntityService,
        feature_service: FeatureService,
        default_version_mode_service: DefaultVersionModeService,
        feature_readiness_service: FeatureReadinessService,
        info_service: InfoService,
    ):
        super().__init__(service)
        self.entity_service = entity_service
        self.feature_service = feature_service
        self.default_version_mode_service = default_version_mode_service
        self.feature_readiness_service = feature_readiness_service
        self.info_service = info_service

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
            primary_entity_ids=await self.derive_primary_entity_ids(entity_ids=document.entity_ids),
        )
        return cast(Document, output)

    async def list(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        **kwargs: Any,
    ) -> PaginatedDocument:
        document_data = await self.service.list_documents(
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
        entity_id_to_entity = await self.get_entity_id_to_entity(doc_list=document_data["data"])

        feature_id_to_primary_table_ids = {}
        async for feature_dict in self.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(default_feature_ids)}}
        ):
            feature = FeatureModel(**feature_dict)
            feature_id_to_primary_table_ids[feature.id] = feature.primary_table_ids

        # construct primary entity IDs and primary table IDs & add these attributes to feature namespace docs
        output = []
        for feature_namespace in document_data["data"]:
            primary_entity_ids = await self.derive_primary_entity_ids(
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
        info_document = await self.info_service.get_feature_namespace_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
