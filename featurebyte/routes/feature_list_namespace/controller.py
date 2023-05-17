"""
FeatureListNamespace API route controller
"""
from __future__ import annotations

from typing import Any, Literal, cast

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.routes.common.base import (
    BaseDocumentController,
    DerivePrimaryEntityMixin,
    PaginatedDocument,
)
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceList,
    FeatureListNamespaceModelResponse,
    FeatureListNamespaceServiceUpdate,
    FeatureListNamespaceUpdate,
)
from featurebyte.schema.info import FeatureListNamespaceInfo
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_list_status import FeatureListStatusService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.info import InfoService
from featurebyte.service.mixin import Document


class FeatureListNamespaceController(
    BaseDocumentController[
        FeatureListNamespaceModelResponse, FeatureListNamespaceService, FeatureListNamespaceList
    ],
    DerivePrimaryEntityMixin,
):
    """
    FeatureList controller
    """

    paginated_document_class = FeatureListNamespaceList

    def __init__(
        self,
        service: FeatureListNamespaceService,
        entity_service: EntityService,
        feature_list_service: FeatureListService,
        default_version_mode_service: DefaultVersionModeService,
        feature_readiness_service: FeatureReadinessService,
        feature_list_status_service: FeatureListStatusService,
        info_service: InfoService,
    ):
        super().__init__(service)
        self.entity_service = entity_service
        self.feature_list_service = feature_list_service
        self.default_version_mode_service = default_version_mode_service
        self.feature_readiness_service = feature_readiness_service
        self.feature_list_status_service = feature_list_status_service
        self.info_service = info_service

    async def get(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
    ) -> Document:
        document = await self.service.get_document(
            document_id=document_id, exception_detail=exception_detail
        )
        output = FeatureListNamespaceModelResponse(
            **document.json_dict(),
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

        # compute primary entity ids of each feature list namespace
        entity_id_to_entity = await self.get_entity_id_to_entity(doc_list=document_data["data"])
        output = []
        for feature_list_namespace in document_data["data"]:
            primary_entity_ids = await self.derive_primary_entity_ids(
                entity_ids=feature_list_namespace["entity_ids"],
                entity_id_to_entity=entity_id_to_entity,
            )
            output.append(
                FeatureListNamespaceModelResponse(
                    **feature_list_namespace,
                    primary_entity_ids=primary_entity_ids,
                )
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

        Raises
        ------
        DocumentUpdateError
            When the new feature list version creation fails
        """
        if data.default_version_mode:
            await self.default_version_mode_service.update_feature_list_namespace(
                feature_list_namespace_id=feature_list_namespace_id,
                default_version_mode=data.default_version_mode,
                return_document=False,
            )

        if data.default_feature_list_id:
            feature_list_namespace = await self.service.get_document(
                document_id=feature_list_namespace_id
            )
            if feature_list_namespace.default_version_mode != DefaultVersionMode.MANUAL:
                raise DocumentUpdateError(
                    "Cannot set default feature list ID when default version mode is not MANUAL."
                )

            # update feature list namespace default feature list ID and update feature readiness
            await self.service.update_document(
                document_id=feature_list_namespace_id,
                data=FeatureListNamespaceServiceUpdate(
                    default_feature_list_id=data.default_feature_list_id
                ),
            )
            await self.feature_readiness_service.update_feature_list_namespace(
                feature_list_namespace_id=feature_list_namespace_id
            )

        if data.status:
            await self.feature_list_status_service.update_feature_list_namespace_status(
                feature_list_namespace_id=feature_list_namespace_id,
                target_feature_list_status=data.status,
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
        info_document = await self.info_service.get_feature_list_namespace_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
